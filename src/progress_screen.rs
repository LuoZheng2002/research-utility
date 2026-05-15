use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    text::Line,
    widgets::{Block, Borders, Gauge, Paragraph, Wrap},
};
use tokio::sync::mpsc;

use crate::message::WorkerMessage;

pub const PROGRESS_SCREEN_KEY_ORDER: &[&str] = &[
    "status",
];

pub struct ProgressScreenConfig {
    pub window_title: String,
    pub num_workers: usize,
    pub steps_per_worker: usize,
    pub key_order: Vec<String>,
    pub redraw_interval: Duration,
    pub persist_after_channel_close: bool,
    pub persist_exit_hint: String,
}

pub struct ProgressScreen {
    // config: ProgressScreenConfig,
    message_tx: mpsc::UnboundedSender<WorkerMessage>,
    // message_rx: mpsc::UnboundedReceiver<WorkerMessage>,
    pub join_handle: Option<tokio::task::JoinHandle<io::Result<()>>>,
}

impl ProgressScreen {
    pub fn new(config: ProgressScreenConfig) -> Arc<Self> {
        let (message_tx, message_rx) = mpsc::unbounded_channel();
        let join_handle = tokio::spawn(async move { Self::run(config, message_rx).await });
        Arc::new(Self {
            // config,
            message_tx,
            // message_rx,
            join_handle: Some(join_handle),
        })
    }
    pub fn receive_message(&self, message: WorkerMessage) {
        self.message_tx.send(message).expect("Failed to send message to ProgressScreen");
    }

    pub fn clone_message_tx(&self) -> mpsc::UnboundedSender<WorkerMessage> {
        self.message_tx.clone()
    }
    pub async fn run(
        config: ProgressScreenConfig,
        mut worker_message_rx: mpsc::UnboundedReceiver<WorkerMessage>,
    ) -> io::Result<()> {
        assert!(config.num_workers > 0, "num_workers must be > 0");
        assert!(config.steps_per_worker > 0, "steps_per_worker must be > 0");
        assert!(
            !config.key_order.is_empty(),
            "key_order must contain at least one key"
        );

        let _terminal_guard = TerminalGuard::enter()?;
        let backend = CrosstermBackend::new(io::stdout());
        let mut terminal = Terminal::new(backend)?;
        terminal.clear()?;

        let mut state = ProgressScreenState::new(config.num_workers);
        let mut redraw_interval = tokio::time::interval(config.redraw_interval);
        let mut worker_channel_closed = false;

        loop {
            tokio::select! {
                recv_result = worker_message_rx.recv() => {
                    match recv_result {
                        Some(message) => state.handle_message(message),
                        None => {
                            worker_channel_closed = true;
                            if !config.persist_after_channel_close {
                                break;
                            }
                        }
                    }
                    draw(&mut terminal, &state, &config, worker_channel_closed)?;
                }
                _ = redraw_interval.tick() => {
                    while event::poll(Duration::from_millis(0))? {
                        if let Event::Key(key_event) = event::read()? {
                            if key_event.code == KeyCode::Char('Q')
                                || key_event.code == KeyCode::Char('q')
                            {
                                return Ok(());
                            }
                        }
                    }
                    draw(&mut terminal, &state, &config, worker_channel_closed)?;
                }
            }
        }

        Ok(())
    }
}

impl ProgressScreenConfig {
    pub fn from_defaults(num_workers: usize, steps_per_worker: usize) -> Self {
        assert!(num_workers > 0, "num_workers must be > 0");
        assert!(steps_per_worker > 0, "steps_per_worker must be > 0");
        Self {
            window_title: "Progress Window".to_string(),
            num_workers,
            steps_per_worker,
            key_order: PROGRESS_SCREEN_KEY_ORDER
                .iter()
                .map(|key| (*key).to_string())
                .collect(),
            redraw_interval: Duration::from_millis(100),
            persist_after_channel_close: false,
            persist_exit_hint: "All tasks are done. Press Q to exit.".to_string(),
        }
    }
}

struct ProgressScreenState {
    key_values: HashMap<String, String>,
    worker_progress: Vec<f32>,
    worker_labels: Vec<String>,
    master_progress: f32,
    master_label: String,
}

impl ProgressScreenState {
    fn new(num_workers: usize) -> Self {
        assert!(num_workers > 0, "num_workers must be > 0");
        Self {
            key_values: HashMap::new(),
            worker_progress: vec![0.0; num_workers],
            worker_labels: vec!["0%".to_string(); num_workers],
            master_progress: 0.0,
            master_label: "0%".to_string(),
        }
    }

    fn handle_message(&mut self, message: WorkerMessage) {
        match message {
            WorkerMessage::KeyValuePair { key, value } => {
                self.key_values.insert(key, value);
            }
            WorkerMessage::WorkerProgress {
                worker_id,
                progress,
                label,
            } => {
                assert!(
                    (0.0..=1.0).contains(&progress),
                    "WorkerProgress.progress must be in [0, 1], got {}",
                    progress
                );
                assert!(
                    worker_id < self.worker_progress.len(),
                    "worker_id out of range in WorkerProgress: {} >= {}",
                    worker_id,
                    self.worker_progress.len()
                );
                self.worker_progress[worker_id] = progress;
                self.worker_labels[worker_id] = label;
            }
            WorkerMessage::MasterProgress { progress, label } => {
                assert!(
                    (0.0..=1.0).contains(&progress),
                    "MasterProgress.progress must be in [0, 1], got {}",
                    progress
                );
                self.master_progress = progress;
                self.master_label = label;
            }
        }
    }
}

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        execute!(io::stdout(), EnterAlternateScreen)?;
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

fn draw(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: &ProgressScreenState,
    config: &ProgressScreenConfig,
    worker_channel_closed: bool,
) -> io::Result<()> {
    terminal.draw(|frame| {
        let area = frame.area();
        let show_persist_exit_hint = config.persist_after_channel_close && worker_channel_closed;
        let main_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(6),
                Constraint::Length((config.num_workers as u16) + 2),
                Constraint::Length(3),
                Constraint::Length(if show_persist_exit_hint { 3 } else { 0 }),
            ])
            .split(area);

        let ordered_lines = ordered_key_value_lines(&state.key_values, &config.key_order);
        let window = Paragraph::new(ordered_lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(config.window_title.as_str()),
            )
            .wrap(Wrap { trim: false });
        frame.render_widget(window, main_layout[0]);

        let workers_block = Block::default().borders(Borders::ALL).title("Endpoints");
        let workers_inner = workers_block.inner(main_layout[1]);
        frame.render_widget(workers_block, main_layout[1]);

        let worker_rows: Vec<Constraint> = (0..config.num_workers)
            .map(|_| Constraint::Length(1))
            .collect();
        let worker_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints(worker_rows)
            .split(workers_inner);

        for worker_id in 0..config.num_workers {
            let ratio = state.worker_progress[worker_id] as f64;
            let gauge = Gauge::default()
                .label(format!(
                    "Endpoint {}: {}",
                    worker_id + 1,
                    state.worker_labels[worker_id]
                ))
                .ratio(ratio)
                .style(Style::default().fg(Color::LightBlue));
            frame.render_widget(gauge, worker_layout[worker_id]);
        }

        let master_ratio = state.master_progress as f64;
        let master_gauge = Gauge::default()
            .block(Block::default().borders(Borders::ALL).title("Master"))
            .label(format!("Master: {}", state.master_label))
            .ratio(master_ratio)
            .style(Style::default().fg(Color::Green));
        frame.render_widget(master_gauge, main_layout[2]);

        if show_persist_exit_hint {
            let hint = Paragraph::new(config.persist_exit_hint.as_str()).block(
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
