use std::collections::{BTreeMap, HashSet, VecDeque};
use std::io;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers,
        MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use lru::LruCache;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, Paragraph, Wrap},
};
use std::num::NonZeroUsize;

use crate::bincode_log_file::BincodeLogFile;
use crate::message::Severity;
use crate::progress_tui_logger::{ProgressGaugeState, ProgressLogFrame};

const WINDOW_TITLE: &str = "Progress Screen";
const DEFAULT_REDRAW_INTERVAL: Duration = Duration::from_millis(100);
const FRAME_UPDATE_INTERVAL: Duration = Duration::from_millis(500);
const MAX_LOG_LINES: usize = 100;
const KEY_ORDER: &[&str] = &["status"];
const SPEED_STEP_FRAMES_PER_HALF_SECOND: i32 = 1;
const DEFAULT_SPEED_FRAMES_PER_HALF_SECOND: i32 = 1;
const CACHE_CAPACITY: usize = 48;
const CACHE_STRIDE: usize = 20;
const REFRESH_NOTICE_DURATION: Duration = Duration::from_millis(1200);

pub async fn run(log_file_path: impl Into<PathBuf>) -> io::Result<()> {
    run_with_redraw_interval_and_sync_script(log_file_path, DEFAULT_REDRAW_INTERVAL, None).await
}

pub async fn run_with_redraw_interval(
    log_file_path: impl Into<PathBuf>,
    redraw_interval_duration: Duration,
) -> io::Result<()> {
    run_with_redraw_interval_and_sync_script(log_file_path, redraw_interval_duration, None).await
}

pub async fn run_with_redraw_interval_and_sync_script(
    log_file_path: impl Into<PathBuf>,
    redraw_interval_duration: Duration,
    sync_script_path: Option<String>,
) -> io::Result<()> {
    let log_file_path = log_file_path.into();
    if !log_file_path.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "progress log file does not exist: {}",
                log_file_path.display()
            ),
        ));
    }

    let _terminal_guard = TerminalGuard::enter()?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let mut replay = ReplayEngine::open(log_file_path)
        .map_err(|e| io::Error::other(format!("failed to open replay log: {e}")))?;
    let mut screen_state = ProgressScreenState::new();

    let mut redraw_interval = tokio::time::interval(redraw_interval_duration);
    let mut frame_update_interval = tokio::time::interval(FRAME_UPDATE_INTERVAL);

    loop {
        tokio::select! {
            _ = redraw_interval.tick() => {
                let actions = handle_input_events(&mut screen_state)?;
                if actions.quit {
                    break;
                }
                let refresh_requested = actions.refresh_file;
                replay.apply_actions(actions);

                if refresh_requested {
                    let had_sync_script = sync_script_path.is_some();
                    if let Some(script_path) = sync_script_path.as_deref() {
                        if let Err(err) = run_sync_script(script_path) {
                            screen_state.show_refresh_notice(
                                format!("Sync script failed: {err}"),
                                Color::Red,
                            );
                            draw(&mut terminal, &mut screen_state, replay.playback_status())?;
                            continue;
                        }
                    }
                    match replay.force_refresh() {
                        Ok(result) => {
                            let verb = if had_sync_script {
                                "Synced+refreshed"
                            } else {
                                "Refreshed file"
                            };
                            let message = if result.current_frame_count > result.previous_frame_count
                            {
                                format!(
                                    "{}: {} -> {} frames",
                                    verb,
                                    result.previous_frame_count, result.current_frame_count
                                )
                            } else {
                                format!("{}: {} frames", verb, result.current_frame_count)
                            };
                            screen_state.show_refresh_notice(message, Color::Green);
                        }
                        Err(err) => {
                            screen_state
                                .show_refresh_notice(format!("Refresh failed: {err}"), Color::Red);
                        }
                    }
                }

                if let Some(frame_state) = replay.current_frame_state().map_err(io::Error::other)? {
                    screen_state.apply_replay_state(frame_state);
                } else {
                    screen_state.apply_replay_state(ReplayFrameState::default());
                }

                draw(&mut terminal, &mut screen_state, replay.playback_status())?;
            }
            _ = frame_update_interval.tick() => {
                replay.advance(1).map_err(io::Error::other)?;
            }
        }
    }

    Ok(())
}

fn run_sync_script(script_path: &str) -> Result<(), String> {
    let output = Command::new("bash")
        .arg(script_path)
        .output()
        .map_err(|err| format!("failed to execute script '{}': {}", script_path, err))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.trim();
    if stderr.is_empty() {
        return Err(format!(
            "script '{}' exited with status {}",
            script_path, output.status
        ));
    }
    Err(format!(
        "script '{}' exited with status {}: {}",
        script_path, output.status, stderr
    ))
}

#[derive(Debug, Clone)]
struct ReplayFrameState {
    elapsed_seconds: f64,
    state_text: String,
    window_name: String,
    exit_hint: String,
    key_values: BTreeMap<String, String>,
    worker_progress: BTreeMap<String, ProgressGaugeState>,
    master_progress: ProgressGaugeState,
    log_lines: VecDeque<LogLine>,
}

impl Default for ReplayFrameState {
    fn default() -> Self {
        Self {
            elapsed_seconds: 0.0,
            state_text: String::new(),
            window_name: WINDOW_TITLE.to_string(),
            exit_hint: "Reader active. Ctrl+C to exit.".to_string(),
            key_values: BTreeMap::new(),
            worker_progress: BTreeMap::new(),
            master_progress: ProgressGaugeState {
                progress: 0.0,
                label: "0%".to_string(),
            },
            log_lines: VecDeque::new(),
        }
    }
}

impl ReplayFrameState {
    fn apply_frame(&mut self, frame: ProgressLogFrame) {
        self.elapsed_seconds = frame.elapsed_seconds;
        if let Some(state) = frame.state {
            self.state_text = state;
        }
        if let Some(window_name) = frame.window_name {
            self.window_name = window_name;
        }
        if let Some(exit_hint) = frame.exit_hint {
            self.exit_hint = exit_hint;
        }
        for (key, value) in frame.key_values {
            self.key_values.insert(key, value);
        }
        for (worker_name, gauge) in frame.worker_progress {
            self.worker_progress.insert(worker_name, gauge);
        }
        for worker_name in frame.deleted_workers {
            self.worker_progress.remove(&worker_name);
        }
        if let Some(master_progress) = frame.master_progress {
            self.master_progress = master_progress;
        }
        for log_line in frame.log_lines {
            self.log_lines.push_back(LogLine {
                message: log_line.message,
                severity: log_line.severity,
            });
            if self.log_lines.len() > MAX_LOG_LINES {
                self.log_lines.pop_front();
            }
        }
    }
}

struct ReplayEngine {
    log: BincodeLogFile<ProgressLogFrame>,
    frame_count: usize,
    current_frame: usize,
    play_speed_frames_per_half_second: i32,
    is_paused: bool,
    pause_at_end_resume_speed: i32,
    auto_paused_at_end: bool,
    state_cache: LruCache<usize, ReplayFrameState>,
}

struct RefreshResult {
    previous_frame_count: usize,
    current_frame_count: usize,
}

impl ReplayEngine {
    fn open(log_path: PathBuf) -> Result<Self, String> {
        let mut log = BincodeLogFile::open(log_path)?;
        let frame_count = log.reload()?;
        let capacity = NonZeroUsize::new(CACHE_CAPACITY).expect("cache capacity must be non-zero");

        Ok(Self {
            log,
            frame_count,
            current_frame: 0,
            play_speed_frames_per_half_second: DEFAULT_SPEED_FRAMES_PER_HALF_SECOND,
            is_paused: false,
            pause_at_end_resume_speed: DEFAULT_SPEED_FRAMES_PER_HALF_SECOND,
            auto_paused_at_end: false,
            state_cache: LruCache::new(capacity),
        })
    }

    fn apply_actions(&mut self, actions: InputActions) {
        if actions.toggle_pause {
            self.is_paused = true;
            self.play_speed_frames_per_half_second = 0;
            self.auto_paused_at_end = false;
        }
        if actions.speed_delta_steps != 0 {
            self.play_speed_frames_per_half_second +=
                actions.speed_delta_steps * SPEED_STEP_FRAMES_PER_HALF_SECOND;
            self.is_paused = false;
            self.auto_paused_at_end = false;
            if self.play_speed_frames_per_half_second > 0 {
                self.pause_at_end_resume_speed = self.play_speed_frames_per_half_second;
            }
        }
    }

    fn advance(&mut self, delta_frames: usize) -> Result<(), String> {
        self.refresh_if_on_last_frame()?;

        if self.is_paused
            || self.play_speed_frames_per_half_second == 0
            || self.frame_count == 0
            || delta_frames == 0
        {
            return Ok(());
        }

        let delta_frames_i64 = i64::try_from(delta_frames)
            .map_err(|_| "delta_frames exceeds i64 range".to_string())?;
        let speed = i64::from(self.play_speed_frames_per_half_second);
        let frame_delta = speed
            .checked_mul(delta_frames_i64)
            .ok_or_else(|| "frame delta overflow".to_string())?;
        let current = i64::try_from(self.current_frame)
            .map_err(|_| "current_frame exceeds i64 range".to_string())?;
        let mut target = current
            .checked_add(frame_delta)
            .ok_or_else(|| "target frame overflow".to_string())?;

        if target < 0 {
            target = 0;
        }

        let last_frame = i64::try_from(self.frame_count - 1)
            .map_err(|_| "frame_count exceeds i64 range".to_string())?;
        if target > last_frame {
            self.refresh_if_on_last_frame()?;
            let refreshed_last_frame = i64::try_from(self.frame_count - 1)
                .map_err(|_| "frame_count exceeds i64 range".to_string())?;
            if target > refreshed_last_frame {
                self.current_frame = self.frame_count - 1;
                self.is_paused = true;
                self.auto_paused_at_end = true;
                if self.play_speed_frames_per_half_second > 0 {
                    self.pause_at_end_resume_speed = self.play_speed_frames_per_half_second;
                }
                self.play_speed_frames_per_half_second = 0;
                return Ok(());
            }
        }

        self.current_frame = usize::try_from(target)
            .map_err(|_| "target frame is negative or exceeds usize range".to_string())?;

        Ok(())
    }

    fn refresh_if_on_last_frame(&mut self) -> Result<(), String> {
        if self.frame_count == 0 || self.current_frame + 1 >= self.frame_count {
            self.reload_frames()?;
        }
        Ok(())
    }

    fn force_refresh(&mut self) -> Result<RefreshResult, String> {
        let previous_frame_count = self.frame_count;
        self.reload_frames()?;
        Ok(RefreshResult {
            previous_frame_count,
            current_frame_count: self.frame_count,
        })
    }

    fn reload_frames(&mut self) -> Result<(), String> {
        let previous_len = self.frame_count;
        self.frame_count = self.log.reload()?;
        self.state_cache.clear();

        if self.frame_count == 0 {
            self.current_frame = 0;
            return Ok(());
        }

        if self.current_frame >= self.frame_count {
            self.current_frame = self.frame_count - 1;
        }

        if self.auto_paused_at_end && self.frame_count > previous_len {
            self.play_speed_frames_per_half_second = self.pause_at_end_resume_speed.max(1);
            self.is_paused = false;
            self.auto_paused_at_end = false;
        }

        Ok(())
    }

    fn current_frame_state(&mut self) -> Result<Option<ReplayFrameState>, String> {
        if self.frame_count == 0 {
            return Ok(None);
        }

        let target = self.current_frame;
        if let Some(cached) = self.state_cache.get(&target).cloned() {
            return Ok(Some(cached));
        }

        let mut best_anchor: Option<(usize, ReplayFrameState)> = None;
        for (cached_frame, cached_state) in self.state_cache.iter() {
            if *cached_frame <= target {
                if let Some((best_frame, _)) = &best_anchor {
                    if *cached_frame > *best_frame {
                        best_anchor = Some((*cached_frame, cached_state.clone()));
                    }
                } else {
                    best_anchor = Some((*cached_frame, cached_state.clone()));
                }
            }
        }

        let (mut state, start_frame) = if let Some((frame, state)) = best_anchor {
            (state, frame + 1)
        } else {
            (ReplayFrameState::default(), 0)
        };

        for frame_index in start_frame..=target {
            let Some(frame) = self.log.get(frame_index)? else {
                self.frame_count = frame_index;
                if self.frame_count == 0 {
                    self.current_frame = 0;
                    self.state_cache.clear();
                    return Ok(None);
                }

                if self.current_frame >= self.frame_count {
                    self.current_frame = self.frame_count - 1;
                }
                self.state_cache.clear();
                self.state_cache.put(self.current_frame, state.clone());
                return Ok(Some(state));
            };
            state.apply_frame(frame);

            if frame_index % CACHE_STRIDE == 0 || frame_index == target {
                self.state_cache.put(frame_index, state.clone());
            }
        }

        Ok(Some(state))
    }

    fn playback_status(&self) -> PlaybackStatus {
        PlaybackStatus {
            frame_count: self.frame_count,
            current_frame: self.current_frame,
            speed_frames_per_half_second: self.play_speed_frames_per_half_second,
            paused: self.is_paused,
        }
    }
}

#[derive(Default)]
struct InputActions {
    quit: bool,
    toggle_pause: bool,
    speed_delta_steps: i32,
    refresh_file: bool,
}

#[derive(Clone, Copy)]
struct PlaybackStatus {
    frame_count: usize,
    current_frame: usize,
    speed_frames_per_half_second: i32,
    paused: bool,
}

struct ProgressScreenState {
    elapsed_seconds: f64,
    state_text: String,
    window_name: String,
    exit_hint: String,
    key_values: BTreeMap<String, String>,
    stats_scroll_from_bottom: usize,
    stats_viewport_height: usize,
    log_lines: VecDeque<LogLine>,
    log_scroll_from_bottom: usize,
    log_viewport_height: usize,
    stats_area: Rect,
    log_area: Rect,
    worker_progress: BTreeMap<String, ProgressGaugeState>,
    master_progress: ProgressGaugeState,
    refresh_notice: Option<RefreshNotice>,
}

struct RefreshNotice {
    message: String,
    color: Color,
    created_at: Instant,
}

#[derive(Debug, Clone)]
struct LogLine {
    message: String,
    severity: Severity,
}

impl ProgressScreenState {
    fn new() -> Self {
        Self {
            elapsed_seconds: 0.0,
            state_text: String::new(),
            window_name: WINDOW_TITLE.to_string(),
            exit_hint: "Reader active. Ctrl+C to exit.".to_string(),
            key_values: BTreeMap::new(),
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
            refresh_notice: None,
        }
    }

    fn apply_replay_state(&mut self, replay: ReplayFrameState) {
        self.elapsed_seconds = replay.elapsed_seconds;
        self.state_text = replay.state_text;
        self.window_name = replay.window_name;
        self.exit_hint = replay.exit_hint;
        self.key_values = replay.key_values;
        self.worker_progress = replay.worker_progress;
        self.master_progress = replay.master_progress;
        self.log_lines = replay.log_lines;
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

    fn show_refresh_notice(&mut self, message: String, color: Color) {
        self.refresh_notice = Some(RefreshNotice {
            message,
            color,
            created_at: Instant::now(),
        });
    }

    fn refresh_notice_line(&mut self) -> Option<Line<'static>> {
        if let Some(notice) = &self.refresh_notice {
            if notice.created_at.elapsed() <= REFRESH_NOTICE_DURATION {
                return Some(Line::from(Span::styled(
                    notice.message.clone(),
                    Style::default().fg(notice.color),
                )));
            }
        }

        self.refresh_notice = None;
        None
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
                    KeyCode::Char(' ') => {
                        actions.toggle_pause = true;
                        actions.refresh_file = true;
                    }
                    KeyCode::Char('p') | KeyCode::Char('P') => {
                        actions.toggle_pause = true;
                    }
                    KeyCode::Left => {
                        actions.speed_delta_steps -= 1;
                    }
                    KeyCode::Right => {
                        actions.speed_delta_steps += 1;
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
    playback: PlaybackStatus,
) -> io::Result<()> {
    terminal.draw(|frame| {
        let area = frame.area();
        let main_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(6),
                Constraint::Length((state.worker_progress.len() as u16) + 2),
                Constraint::Length(3),
                Constraint::Length(5),
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

        if !state.worker_progress.is_empty() {
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
        }

        let master_ratio = state.master_progress.progress as f64;
        let master_gauge = Gauge::default()
            .block(Block::default().borders(Borders::ALL).title("Master"))
            .label(format!("Master: {}", state.master_progress.label))
            .ratio(master_ratio)
            .style(Style::default().fg(Color::Green));
        frame.render_widget(master_gauge, main_layout[2]);

        let current_frame_display = if playback.frame_count == 0 {
            0
        } else {
            playback.current_frame.saturating_add(1)
        };
        let replay_status_line = format!(
            "Replay: ({}/{})",
            current_frame_display, playback.frame_count
        );
        let playback_hint = if playback.paused {
            vec![
                Line::from(replay_status_line),
                Line::from(format!(
                    "t={:.1}s | speed 0 frame/0.5s (paused) | Space pause+refresh | <-/-> +/-1 frame/0.5s",
                    state.elapsed_seconds,
                )),
            ]
        } else {
            vec![
                Line::from(replay_status_line),
                Line::from(format!(
                    "t={:.1}s | speed {} frame/0.5s | Space pause+refresh | <-/-> +/-1 frame/0.5s",
                    state.elapsed_seconds, playback.speed_frames_per_half_second
                )),
            ]
        };
        let mut playback_lines = playback_hint;
        if let Some(refresh_notice_line) = state.refresh_notice_line() {
            playback_lines.push(refresh_notice_line);
        }
        let footer = Paragraph::new(playback_lines)
            .block(Block::default().borders(Borders::ALL).title("Playback"));
        frame.render_widget(footer, main_layout[3]);
    })?;
    Ok(())
}

fn ordered_key_value_lines(key_values: &BTreeMap<String, String>) -> Vec<Line<'static>> {
    let known_key_set: HashSet<&str> = KEY_ORDER.iter().copied().collect();

    let mut ordered_keys: Vec<&str> = KEY_ORDER
        .iter()
        .copied()
        .filter(|key| key_values.contains_key(*key))
        .collect();

    let unknown_keys: Vec<&str> = key_values
        .keys()
        .map(String::as_str)
        .filter(|key| !known_key_set.contains(*key))
        .collect();

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
