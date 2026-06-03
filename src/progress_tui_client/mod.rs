use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io;
use std::time::Duration;

use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers,
        MouseEventKind,
    },
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
use tokio::net::TcpStream;

use crate::message::{Severity, TuiMessage};
use crate::progress_tui_protocol::{
    ProgressClientMessage, ProgressGaugeState, ProgressServerMessage, ProgressStats,
    framed_reader, framed_writer, read_framed_message, send_framed_message,
};

const WINDOW_TITLE: &str = "Progress Screen";
const MAX_LOG_LINES: usize = 100;
const DEFAULT_REDRAW_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_SERVER_DISCONNECTED_HINT: &str = "Server disconnected. Ctrl+C to exit.";
const KEY_ORDER: &[&str] = &["status"];

pub async fn run(addr: String) -> io::Result<()> {
    run_with_redraw_interval(addr, DEFAULT_REDRAW_INTERVAL).await
}

pub async fn run_with_redraw_interval(addr: String, redraw_interval_duration: Duration) -> io::Result<()> {
    let _terminal_guard = TerminalGuard::enter()?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let stream = TcpStream::connect(&addr).await?;
    let (reader, writer) = stream.into_split();
    let mut reader = framed_reader(reader);
    let mut writer = framed_writer(writer);
    let (client_message_tx, mut client_message_rx) =
        tokio::sync::mpsc::unbounded_channel::<ProgressClientMessage>();
    client_message_tx
        .send(ProgressClientMessage::SnapshotRequest)
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "failed to queue snapshot request",
            )
        })?;

    let writer_task = tokio::spawn(async move {
        while let Some(message) = client_message_rx.recv().await {
            send_framed_message(&mut writer, &message).await?;
        }
        Ok::<(), io::Error>(())
    });

    let mut state = ProgressScreenState::new();
    let mut redraw_interval = tokio::time::interval(redraw_interval_duration);
    let mut server_disconnected = false;

    loop {
        tokio::select! {
            wire_message = read_framed_message::<_, ProgressServerMessage>(&mut reader), if !server_disconnected => {
                match wire_message? {
                    Some(ProgressServerMessage::Snapshot(snapshot)) => {
                        state.apply_snapshot(snapshot);
                    }
                    Some(ProgressServerMessage::Update(message)) => {
                        state.handle_message(message);
                    }
                    None => {
                        server_disconnected = true;
                    }
                }
                let actions = handle_input_events(&mut state)?;
                if actions.quit {
                    break;
                }
                for command in actions.commands {
                    let _ = client_message_tx.send(ProgressClientMessage::SubmitCommand { command });
                }
            }
            _ = redraw_interval.tick() => {
                let actions = handle_input_events(&mut state)?;
                if actions.quit {
                    break;
                }
                for command in actions.commands {
                    let _ = client_message_tx.send(ProgressClientMessage::SubmitCommand { command });
                }
                draw(&mut terminal, &mut state, server_disconnected)?;
            }
        }
    }

    drop(client_message_tx);
    match writer_task.await {
        Ok(result) => result?,
        Err(err) => {
            return Err(io::Error::other(format!("writer task join error: {err}")));
        }
    }

    Ok(())
}

struct ProgressScreenState {
    state_text: String,
    window_name: String,
    exit_hint: String,
    key_values: HashMap<String, String>,
    stats_scroll_from_bottom: usize,
    stats_viewport_height: usize,
    log_lines: VecDeque<LogLine>,
    log_scroll_from_bottom: usize,
    log_viewport_height: usize,
    stats_area: Rect,
    log_area: Rect,
    worker_progress: BTreeMap<String, ProgressGaugeState>,
    master_progress: ProgressGaugeState,
    command_input: String,
}

struct LogLine {
    message: String,
    severity: Severity,
}

impl ProgressScreenState {
    fn new() -> Self {
        Self {
            state_text: String::new(),
            window_name: WINDOW_TITLE.to_string(),
            exit_hint: DEFAULT_SERVER_DISCONNECTED_HINT.to_string(),
            key_values: HashMap::new(),
            stats_scroll_from_bottom: 0,
            stats_viewport_height: 1,
            log_lines: VecDeque::new(),
            log_scroll_from_bottom: 0,
            log_viewport_height: 1,
            stats_area: Rect::default(),
            log_area: Rect::default(),
            worker_progress: BTreeMap::new(),
            master_progress: ProgressGaugeState {
                progress: 0.0,
                label: "0%".to_string(),
            },
            command_input: String::new(),
        }
    }

    fn apply_snapshot(&mut self, snapshot: ProgressStats) {
        self.state_text = snapshot.state;
        self.window_name = snapshot.window_name;
        self.exit_hint = snapshot.exit_hint;
        self.key_values = snapshot.key_values;
        self.worker_progress = snapshot.worker_progress;
        self.master_progress = snapshot.master_progress;
    }

    fn handle_message(&mut self, message: TuiMessage) {
        match message {
            TuiMessage::Line { message, severity } => {
                self.push_log_line(message, severity);
            }
            TuiMessage::State { state } => {
                self.state_text = state;
            }
            TuiMessage::WindowName { window_name } => {
                self.window_name = window_name;
            }
            TuiMessage::KeyValuePair { key, value } => {
                self.key_values.insert(key, value);
            }
            TuiMessage::WorkerProgress {
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
                    .insert(worker_name, ProgressGaugeState { progress, label });
            }
            TuiMessage::MasterProgress { progress, label } => {
                assert!(
                    (0.0..=1.0).contains(&progress),
                    "MasterProgress.progress must be in [0, 1], got {}",
                    progress
                );
                self.master_progress = ProgressGaugeState { progress, label };
            }
            TuiMessage::DeleteWorkerBar { worker_name } => {
                self.worker_progress.remove(&worker_name);
            }
            TuiMessage::ExitHint(hint) => {
                self.exit_hint = hint;
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

#[derive(Default)]
struct InputActions {
    quit: bool,
    commands: Vec<String>,
}

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

fn handle_input_events(state: &mut ProgressScreenState) -> io::Result<InputActions> {
    let mut actions = InputActions::default();
    while event::poll(Duration::from_millis(0))? {
        match event::read()? {
            Event::Key(key_event) => {
                if key_event.kind != KeyEventKind::Press {
                    continue;
                }

                let is_ctrl_quit = key_event.modifiers.contains(KeyModifiers::CONTROL)
                    && matches!(
                        key_event.code,
                        KeyCode::Char('c')
                            | KeyCode::Char('C')
                            | KeyCode::Char('q')
                            | KeyCode::Char('Q')
                    );
                if is_ctrl_quit {
                    actions.quit = true;
                    break;
                }

                match key_event.code {
                    KeyCode::Enter => {
                        let command = state.command_input.trim().to_string();
                        if !command.is_empty() {
                            actions.commands.push(command);
                        }
                        state.command_input.clear();
                    }
                    KeyCode::Backspace => {
                        state.command_input.pop();
                    }
                    KeyCode::Esc => {
                        state.command_input.clear();
                    }
                    KeyCode::Char(ch) => {
                        if !key_event
                            .modifiers
                            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                        {
                            state.command_input.push(ch);
                        }
                    }
                    _ => {}
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
    Ok(actions)
}

fn draw(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: &mut ProgressScreenState,
    server_disconnected: bool,
) -> io::Result<()> {
    terminal.draw(|frame| {
        let area = frame.area();
        let main_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(6),
                Constraint::Length((state.worker_progress.len() as u16) + 2),
                Constraint::Length(3),
                Constraint::Length(if server_disconnected { 3 } else { 0 }),
                Constraint::Length(3),
            ])
            .split(area);

        let stats_layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(main_layout[0]);

        let state_has_text = !state.state_text.trim().is_empty();
        let stats_left_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(if state_has_text { 3 } else { 0 }),
                Constraint::Min(3),
            ])
            .split(stats_layout[0]);
        state.set_top_window_areas(stats_left_layout[1], stats_layout[1]);

        if state_has_text {
            let state_window = Paragraph::new(state.state_text.as_str())
                .block(Block::default().borders(Borders::ALL).title("State"))
                .wrap(Wrap { trim: true });
            frame.render_widget(state_window, stats_left_layout[0]);
        }

        let ordered_lines = ordered_key_value_lines(&state.key_values);
        state.stats_viewport_height = log_inner_height(stats_left_layout[1]);
        let stats_inner_width = log_inner_width(stats_left_layout[1]);
        let stats_total_rows = wrapped_lines_height(&ordered_lines, stats_inner_width, false);
        state.clamp_stats_scroll(stats_total_rows);
        let stats_max_scroll = stats_total_rows.saturating_sub(state.stats_viewport_height.max(1));
        let stats_scroll_from_top =
            stats_max_scroll.saturating_sub(state.stats_scroll_from_bottom) as u16;
        let key_value_window = Paragraph::new(ordered_lines)
            .block(Block::default().borders(Borders::ALL).title(format!(
                "{} - Stats (offset {}/{})",
                state.window_name, state.stats_scroll_from_bottom, stats_max_scroll
            )))
            .scroll((stats_scroll_from_top, 0))
            .wrap(Wrap { trim: false });
        frame.render_widget(key_value_window, stats_left_layout[1]);

        state.log_viewport_height = log_inner_height(stats_layout[1]);
        let log_inner_width = log_inner_width(stats_layout[1]);
        let log_lines = rendered_log_lines(&state.log_lines);
        let log_total_rows = wrapped_lines_height(&log_lines, log_inner_width, false);
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

        if server_disconnected {
            let hint = Paragraph::new(state.exit_hint.as_str())
                .block(Block::default().borders(Borders::ALL).title("Disconnected"));
            frame.render_widget(hint, main_layout[3]);
        }

        let command_area = if server_disconnected {
            main_layout[4]
        } else {
            main_layout[3]
        };
        let command_input = Paragraph::new(state.command_input.as_str()).block(
            Block::default()
                .borders(Borders::ALL)
                .title("Command (Enter to send, Esc to clear, Ctrl+C to exit)"),
        );
        frame.render_widget(command_input, command_area);
    })?;
    Ok(())
}

fn ordered_key_value_lines(key_values: &HashMap<String, String>) -> Vec<Line<'static>> {
    let known_key_set: HashSet<&str> = KEY_ORDER.iter().copied().collect();

    let mut ordered_keys: Vec<&str> = KEY_ORDER
        .iter()
        .copied()
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

fn wrapped_lines_height(lines: &[Line<'_>], inner_width: usize, trim: bool) -> usize {
    if inner_width == 0 {
        return 0;
    }

    Paragraph::new(lines.to_vec())
        .wrap(Wrap { trim })
        .line_count(inner_width as u16)
}
