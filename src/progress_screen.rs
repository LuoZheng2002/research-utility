use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, MouseEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, Paragraph, Wrap},
};
use tokio::sync::{mpsc, oneshot};

use crate::message::{MyLogMessage, Severity};

pub const PROGRESS_SCREEN_KEY_ORDER: &[&str] = &["status"];
const MAX_LOG_LINES: usize = 100;
const DEFAULT_REDRAW_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_PERSIST_EXIT_HINT: &str = "All tasks are done.";
const PRESS_Q_TO_EXIT_HINT: &str = " Press Q to exit.";

pub(crate) static PROGRESS_SCREEN_MESSAGE_TX: ArcSwapOption<mpsc::UnboundedSender<MyLogMessage>> =
    ArcSwapOption::const_empty();

struct RunConfig {
    window_title: String,
    key_order: Vec<String>,
    redraw_interval: Duration,
    persist_after_channel_close: bool,
}

pub struct ProgressScreen;

struct ProgressScreenRuntime {
    join_handle: Option<tokio::task::JoinHandle<io::Result<()>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

static PROGRESS_SCREEN_RUNTIME: OnceLock<Mutex<ProgressScreenRuntime>> = OnceLock::new();

static PERSISTENT_EXIT_HINT: LazyLock<Mutex<String>> =
    LazyLock::new(|| Mutex::new(DEFAULT_PERSIST_EXIT_HINT.to_string()));

fn runtime_state() -> &'static Mutex<ProgressScreenRuntime> {
    PROGRESS_SCREEN_RUNTIME.get_or_init(|| {
        Mutex::new(ProgressScreenRuntime {
            join_handle: None,
            shutdown_tx: None,
        })
    })
}

impl ProgressScreen {
    pub async fn initialize(
        window_title: impl Into<String>,
        persist_after_channel_close: bool,
        log_file: Option<String>,
    ) -> io::Result<()> {
        let mut runtime = runtime_state()
            .lock()
            .expect("progress screen runtime mutex poisoned");
        if runtime.join_handle.is_some() {
            return Ok(());
        }

        let config = RunConfig {
            window_title: window_title.into(),
            key_order: PROGRESS_SCREEN_KEY_ORDER
                .iter()
                .map(|key| (*key).to_string())
                .collect(),
            redraw_interval: DEFAULT_REDRAW_INTERVAL,
            persist_after_channel_close,
        };

        let (my_log_message_tx, my_log_message_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        // set_my_log_message_tx(my_log_message_tx);
        PROGRESS_SCREEN_MESSAGE_TX.store(Some(Arc::new(my_log_message_tx)));

        let join_handle = tokio::spawn(async move {
            Self::run(config, my_log_message_rx, shutdown_rx, log_file).await
        });
        runtime.shutdown_tx = Some(shutdown_tx);
        runtime.join_handle = Some(join_handle);
        Ok(())
    }

    pub async fn shutdown() -> io::Result<()> {
        let (shutdown_tx, join_handle) = {
            let mut runtime = runtime_state()
                .lock()
                .expect("progress screen runtime mutex poisoned");
            (runtime.shutdown_tx.take(), runtime.join_handle.take())
        };

        // clear_my_log_message_tx();
        PROGRESS_SCREEN_MESSAGE_TX.store(None);

        if let Some(shutdown_tx) = shutdown_tx {
            let _ = shutdown_tx.send(());
        }

        match join_handle {
            Some(join_handle) => match join_handle.await {
                Ok(result) => result,
                Err(err) => Err(io::Error::other(format!(
                    "progress screen task join error: {err}"
                ))),
            },
            None => Ok(()),
        }
    }
    pub fn set_persist_exit_hint(hint: impl Into<String>) {
        let mut guard = PERSISTENT_EXIT_HINT
            .lock()
            .expect("persistent exit hint mutex poisoned");
        *guard = hint.into();
    }

    async fn run(
        config: RunConfig,
        mut my_log_message_rx: mpsc::UnboundedReceiver<MyLogMessage>,
        mut shutdown_rx: oneshot::Receiver<()>,
        log_file: Option<String>,
    ) -> io::Result<()> {
        let _terminal_guard = TerminalGuard::enter()?;
        let backend = CrosstermBackend::new(io::stdout());
        let mut terminal = Terminal::new(backend)?;
        terminal.clear()?;

        let mut state = ProgressScreenState::new();
        let mut redraw_interval = tokio::time::interval(config.redraw_interval);
        let mut my_log_channel_closed = false;
        let mut shutdown_requested = false;
        let mut log_file_writer = log_file
            .map(|path| {
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(path)
            })
            .transpose()?;

        loop {
            tokio::select! {
                recv_result = my_log_message_rx.recv() => {
                    match recv_result {
                        Some(message) => {
                            write_line_to_log_file_if_enabled(&message, &mut log_file_writer)?;
                            state.handle_message(message)
                        }
                        None => {
                            my_log_channel_closed = true;
                            if !config.persist_after_channel_close {
                                break;
                            }
                        }
                    }
                    if handle_input_events(&mut state)? {
                        return Ok(());
                    }
                    draw(&mut terminal, &mut state, &config, my_log_channel_closed)?;
                }
                _ = &mut shutdown_rx, if !shutdown_requested => {
                    shutdown_requested = true;
                    my_log_channel_closed = true;
                    if !config.persist_after_channel_close {
                        break;
                    }
                    if handle_input_events(&mut state)? {
                        return Ok(());
                    }
                    draw(&mut terminal, &mut state, &config, my_log_channel_closed)?;
                }
                _ = redraw_interval.tick() => {
                    if handle_input_events(&mut state)? {
                        return Ok(());
                    }
                    draw(&mut terminal, &mut state, &config, my_log_channel_closed)?;
                }
            }
        }

        Ok(())
    }
}

struct MasterOrWorkerProgress {
    progress: f32,
    label: String,
}

struct ProgressScreenState {
    key_values: HashMap<String, String>,
    stats_scroll_from_bottom: usize,
    stats_viewport_height: usize,
    log_lines: VecDeque<LogLine>,
    log_scroll_from_bottom: usize,
    log_viewport_height: usize,
    stats_area: Rect,
    log_area: Rect,
    worker_progress: BTreeMap<String, MasterOrWorkerProgress>,
    master_progress: MasterOrWorkerProgress,
}

struct LogLine {
    message: String,
    severity: Severity,
}

impl ProgressScreenState {
    fn new() -> Self {
        Self {
            key_values: HashMap::new(),
            stats_scroll_from_bottom: 0,
            stats_viewport_height: 1,
            log_lines: VecDeque::new(),
            log_scroll_from_bottom: 0,
            log_viewport_height: 1,
            stats_area: Rect::default(),
            log_area: Rect::default(),
            worker_progress: BTreeMap::new(),
            master_progress: MasterOrWorkerProgress {
                progress: 0.0,
                label: "0%".to_string(),
            },
        }
    }

    fn handle_message(&mut self, message: MyLogMessage) {
        match message {
            MyLogMessage::Line { message, severity } => {
                self.push_log_line(message, severity);
            }
            MyLogMessage::KeyValuePair { key, value } => {
                self.key_values.insert(key, value);
            }
            MyLogMessage::WorkerProgress {
                worker_name,
                progress,
                label,
            } => {
                assert!(
                    (0.0..=1.0).contains(&progress),
                    "WorkerProgress.progress must be in [0, 1], got {}",
                    progress
                );
                self.worker_progress
                    .insert(worker_name, MasterOrWorkerProgress { progress, label });
            }
            MyLogMessage::MasterProgress { progress, label } => {
                assert!(
                    (0.0..=1.0).contains(&progress),
                    "MasterProgress.progress must be in [0, 1], got {}",
                    progress
                );
                self.master_progress = MasterOrWorkerProgress { progress, label };
            }
            MyLogMessage::DeleteWorkerBar { worker_name } => {
                self.worker_progress.remove(&worker_name);
            }
        }
    }

    fn push_log_line(&mut self, message: String, severity: Severity) {
        if self.log_scroll_from_bottom > 0 {
            self.log_scroll_from_bottom = self.log_scroll_from_bottom.saturating_add(1);
        }
        self.log_lines.push_back(LogLine { message, severity });
        if self.log_lines.len() > MAX_LOG_LINES {
            self.log_lines.pop_front();
        }
    }

    fn scroll_log_up(&mut self, amount: usize) {
        self.log_scroll_from_bottom = self.log_scroll_from_bottom.saturating_add(amount);
    }

    fn scroll_log_down(&mut self, amount: usize) {
        self.log_scroll_from_bottom = self.log_scroll_from_bottom.saturating_sub(amount);
    }

    fn scroll_stats_up(&mut self, amount: usize) {
        self.stats_scroll_from_bottom = self.stats_scroll_from_bottom.saturating_add(amount);
    }

    fn scroll_stats_down(&mut self, amount: usize) {
        self.stats_scroll_from_bottom = self.stats_scroll_from_bottom.saturating_sub(amount);
    }

    fn scroll_top_window_up(&mut self, mouse_x: u16, mouse_y: u16, amount: usize) {
        if rect_contains(self.stats_area, mouse_x, mouse_y) {
            self.scroll_stats_up(amount);
        } else if rect_contains(self.log_area, mouse_x, mouse_y) {
            self.scroll_log_up(amount);
        }
    }

    fn scroll_top_window_down(&mut self, mouse_x: u16, mouse_y: u16, amount: usize) {
        if rect_contains(self.stats_area, mouse_x, mouse_y) {
            self.scroll_stats_down(amount);
        } else if rect_contains(self.log_area, mouse_x, mouse_y) {
            self.scroll_log_down(amount);
        }
    }

    fn set_top_window_areas(&mut self, stats_area: Rect, log_area: Rect) {
        self.stats_area = stats_area;
        self.log_area = log_area;
    }

    fn clamp_stats_scroll(&mut self, stats_line_count: usize) {
        let max_scroll = stats_line_count.saturating_sub(self.stats_viewport_height.max(1));
        if self.stats_scroll_from_bottom > max_scroll {
            self.stats_scroll_from_bottom = max_scroll;
        }
    }

    fn clamp_log_scroll(&mut self, log_line_count: usize) {
        let max_scroll = log_line_count.saturating_sub(self.log_viewport_height.max(1));
        if self.log_scroll_from_bottom > max_scroll {
            self.log_scroll_from_bottom = max_scroll;
        }
    }
}

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        execute!(io::stdout(), EnterAlternateScreen, EnableMouseCapture)?;
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), DisableMouseCapture, LeaveAlternateScreen);
    }
}

fn handle_input_events(state: &mut ProgressScreenState) -> io::Result<bool> {
    while event::poll(Duration::from_millis(0))? {
        match event::read()? {
            Event::Key(key_event) => {
                if key_event.code == KeyCode::Char('Q') || key_event.code == KeyCode::Char('q') {
                    return Ok(true);
                }
            }
            Event::Mouse(mouse_event) => match mouse_event.kind {
                MouseEventKind::ScrollUp => {
                    state.scroll_top_window_up(mouse_event.column, mouse_event.row, 1)
                }
                MouseEventKind::ScrollDown => {
                    state.scroll_top_window_down(mouse_event.column, mouse_event.row, 1)
                }
                _ => {}
            },
            _ => {}
        }
    }
    Ok(false)
}

fn draw(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: &mut ProgressScreenState,
    config: &RunConfig,
    my_log_channel_closed: bool,
) -> io::Result<()> {
    terminal.draw(|frame| {
        let area = frame.area();
        let show_persist_exit_hint = config.persist_after_channel_close && my_log_channel_closed;
        let main_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(6),
                Constraint::Length((state.worker_progress.len() as u16) + 2),
                Constraint::Length(3),
                Constraint::Length(if show_persist_exit_hint { 3 } else { 0 }),
            ])
            .split(area);

        let stats_layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(main_layout[0]);
        state.set_top_window_areas(stats_layout[0], stats_layout[1]);

        let ordered_lines = ordered_key_value_lines(&state.key_values, &config.key_order);
        state.stats_viewport_height = log_inner_height(stats_layout[0]);
        let stats_inner_width = log_inner_width(stats_layout[0]);
        let stats_total_rows = wrapped_lines_height(&ordered_lines, stats_inner_width);
        state.clamp_stats_scroll(stats_total_rows);
        let stats_max_scroll = stats_total_rows.saturating_sub(state.stats_viewport_height.max(1));
        let stats_scroll_from_top =
            stats_max_scroll.saturating_sub(state.stats_scroll_from_bottom) as u16;
        let key_value_window = Paragraph::new(ordered_lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!(
                        "{} - Stats (offset {}/{})",
                        config.window_title, state.stats_scroll_from_bottom, stats_max_scroll
                    )),
            )
            .scroll((stats_scroll_from_top, 0))
            .wrap(Wrap { trim: false });
        frame.render_widget(key_value_window, stats_layout[0]);

        state.log_viewport_height = log_inner_height(stats_layout[1]);
        let log_inner_width = log_inner_width(stats_layout[1]);
        let log_lines = rendered_log_lines(&state.log_lines);
        let log_total_rows = wrapped_lines_height(&log_lines, log_inner_width);
        state.clamp_log_scroll(log_total_rows);
        let max_scroll = log_total_rows.saturating_sub(state.log_viewport_height.max(1));
        let log_block = Block::default().borders(Borders::ALL).title(format!(
            "Log ({}/{MAX_LOG_LINES}, offset {}/{})",
            state.log_lines.len(),
            state.log_scroll_from_bottom,
            max_scroll
        ));
        let log_scroll_from_top = max_scroll.saturating_sub(state.log_scroll_from_bottom) as u16;
        let log_window = Paragraph::new(log_lines)
            .block(log_block)
            .scroll((log_scroll_from_top, 0))
            .wrap(Wrap { trim: false });
        frame.render_widget(log_window, stats_layout[1]);

        let workers_block = Block::default().borders(Borders::ALL).title("Endpoints");
        let workers_inner = workers_block.inner(main_layout[1]);
        frame.render_widget(workers_block, main_layout[1]);

        let worker_rows: Vec<Constraint> = (0..state.worker_progress.len())
            .map(|_| Constraint::Length(1))
            .collect();
        let worker_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints(worker_rows)
            .split(workers_inner);

        for (i, (worker_name, progress)) in state.worker_progress.iter().enumerate() {
            let ratio = progress.progress as f64;
            let gauge = Gauge::default()
                .label(format!("{}: {}", worker_name, progress.label))
                .ratio(ratio)
                .style(Style::default().fg(Color::LightBlue));
            frame.render_widget(gauge, worker_layout[i]);
        }

        let master_ratio = state.master_progress.progress as f64;
        let master_gauge = Gauge::default()
            .block(Block::default().borders(Borders::ALL).title("Master"))
            .label(format!("Master: {}", state.master_progress.label))
            .ratio(master_ratio)
            .style(Style::default().fg(Color::Green));
        frame.render_widget(master_gauge, main_layout[2]);

        if show_persist_exit_hint {
            let hint =
                Paragraph::new(PERSISTENT_EXIT_HINT.lock().unwrap().clone() + PRESS_Q_TO_EXIT_HINT)
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Program Ended"),
                    );
            frame.render_widget(hint, main_layout[3]);
        }
    })?;
    Ok(())
}

fn ordered_key_value_lines(
    key_values: &HashMap<String, String>,
    key_order: &[String],
) -> Vec<Line<'static>> {
    let known_key_set: HashSet<&str> = key_order.iter().map(String::as_str).collect();

    let mut ordered_keys: Vec<&str> = key_order
        .iter()
        .map(String::as_str)
        .filter(|key| key_values.contains_key(*key))
        .collect();

    let mut unknown_keys: Vec<&str> = key_values
        .keys()
        .map(String::as_str)
        .filter(|key| !known_key_set.contains(*key))
        .collect();
    unknown_keys.sort_unstable();

    ordered_keys.extend(unknown_keys);
    ordered_keys
        .into_iter()
        .map(|key| {
            let value = key_values
                .get(key)
                .expect("key must exist in key_values when rendering");
            Line::from(format!("{key}: {value}"))
        })
        .collect()
}

fn rendered_log_lines(log_lines: &VecDeque<LogLine>) -> Vec<Line<'static>> {
    if log_lines.is_empty() {
        return vec![Line::from("No logs yet")];
    }

    log_lines
        .iter()
        .map(|line| {
            let style = match line.severity {
                Severity::Info => Style::default().fg(Color::White),
                Severity::Warning => Style::default().fg(Color::Yellow),
                Severity::Error => Style::default().fg(Color::Red),
            };
            Line::from(Span::styled(line.message.clone(), style))
        })
        .collect()
}

fn rect_contains(rect: Rect, x: u16, y: u16) -> bool {
    let max_x = rect.x.saturating_add(rect.width);
    let max_y = rect.y.saturating_add(rect.height);
    x >= rect.x && x < max_x && y >= rect.y && y < max_y
}

fn log_inner_height(log_area: ratatui::layout::Rect) -> usize {
    log_area.height.saturating_sub(2) as usize
}

fn log_inner_width(log_area: ratatui::layout::Rect) -> usize {
    log_area.width.saturating_sub(2) as usize
}

fn wrapped_lines_height(lines: &[Line<'_>], inner_width: usize) -> usize {
    if inner_width == 0 {
        return 0;
    }

    lines
        .iter()
        .map(|line| line.width().max(1).div_ceil(inner_width))
        .sum()
}

fn write_line_to_log_file_if_enabled(
    message: &MyLogMessage,
    log_file_writer: &mut Option<std::fs::File>,
) -> io::Result<()> {
    let Some(file) = log_file_writer.as_mut() else {
        return Ok(());
    };

    if let MyLogMessage::Line { message, .. } = message {
        file.write_all(message.as_bytes())?;
        file.write_all(b"\n")?;
    }

    Ok(())
}
