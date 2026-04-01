"""
ui/mockup.py — Sheet Pipeline · Mercado Libre
Glassmorphism UI built with PyQt6.
"""
import sys
import os
import re
import logging
import threading
import queue
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QTextEdit, QFrame,
)
from PyQt6.QtCore import Qt, QTimer, QPoint
from PyQt6.QtGui import (
    QPainter, QColor, QLinearGradient, QRadialGradient,
    QBrush, QPen, QFont,
)

from config import EXTRACT_OWN_ENABLED, PATH_PROCESSED_MARKET, PATH_LOGS
from src.utils.utils import setup_dirs
from src.pipeline import get_target_dates, run_pipeline

# ── Palette ───────────────────────────────────────────────────────────────────
C = {
    "bg":            "#07071a",
    "glass":         "rgba(255,255,255,12)",
    "glass_border":  "rgba(255,255,255,28)",
    "glass_dark":    "rgba(255,255,255,6)",
    "glass_hover":   "rgba(255,255,255,18)",
    "accent":        "#7c6ff7",
    "accent_hover":  "#9589f9",
    "accent_dim":    "rgba(124,111,247,30)",
    "teal":          "#4fc3a1",
    "teal_dim":      "rgba(79,195,161,20)",
    "text":          "#f0f0ff",
    "muted":         "#6868a0",
    "ok":            "#4fc3a1",
    "warn":          "#f5c06a",
    "err":           "#f07878",
    "info":          "#9b96f5",
}

STEPS = [
    ("1", "extract_own",      "Extract Own",       "Download own store sales"),
    ("2", "extract_market",   "Extract Market",    "Download competitor reports"),
    ("3", "transform_market", "Transform Market",  "Classify & group competitor data"),
    ("4", "transform_own",    "Transform Own",     "Enrich & group own sales"),
    ("5", "upload",           "Upload to Sheets",  "Write results to Google Sheets"),
]

STEP_TRIGGERS = [
    ("PROCESSING STORE",         [("extract_own",      "active")]),
    ("DOWNLOADING DATE",         [("extract_own",      "done"), ("extract_market", "active")]),
    ("Loading reference sheets", [("extract_market",   "done"), ("transform_market", "active")]),
    ("Market transform complete",[("transform_market", "done")]),
    ("Starting own transform",   [("transform_own",    "active")]),
    ("Own transform complete",   [("transform_own",    "done")]),
    ("Uploading market data",    [("upload",           "active")]),
    ("Appending own data",       [("upload",           "active")]),
    ("Pipeline complete",        [("upload",           "done")]),
    ("No data to upload",        [("upload",           "done")]),
]

STEP_STATES = {
    "idle":    {"num_bg": "rgba(255,255,255,8)",  "num_border": "rgba(255,255,255,15)", "num_fg": C["muted"],  "card_bg": "rgba(255,255,255,4)"},
    "skipped": {"num_bg": "rgba(255,255,255,4)",  "num_border": "rgba(255,255,255,8)",  "num_fg": "#303050",   "card_bg": "rgba(255,255,255,2)"},
    "active":  {"num_bg": "rgba(124,111,247,35)", "num_border": "rgba(124,111,247,80)", "num_fg": C["accent"], "card_bg": "rgba(124,111,247,12)"},
    "done":    {"num_bg": "rgba(79,195,161,25)",  "num_border": "rgba(79,195,161,70)",  "num_fg": C["teal"],   "card_bg": "rgba(79,195,161,8)"},
    "err":     {"num_bg": "rgba(240,120,120,25)", "num_border": "rgba(240,120,120,70)", "num_fg": C["err"],    "card_bg": "rgba(240,120,120,8)"},
}

# ── Log handler ───────────────────────────────────────────────────────────────
class _UILogHandler(logging.Handler):
    def __init__(self, q: queue.Queue):
        super().__init__()
        self.q = q

    def emit(self, record):
        self.q.put(("log", record))


# ══════════════════════════════════════════════════════════════════════════════
# BACKGROUND WIDGET
# ══════════════════════════════════════════════════════════════════════════════
class GradientBackground(QWidget):
    def paintEvent(self, event):  # noqa: ARG002
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        w, h = self.width(), self.height()

        # Base
        p.fillRect(0, 0, w, h, QColor("#07071a"))

        # Violet glow top-left
        g1 = QRadialGradient(0, 0, w * 0.55)
        g1.setColorAt(0.0, QColor(100, 80, 220, 60))
        g1.setColorAt(1.0, QColor(0, 0, 0, 0))
        p.fillRect(0, 0, w, h, QBrush(g1))

        # Teal glow bottom-right
        g2 = QRadialGradient(w, h, w * 0.5)
        g2.setColorAt(0.0, QColor(40, 180, 140, 45))
        g2.setColorAt(1.0, QColor(0, 0, 0, 0))
        p.fillRect(0, 0, w, h, QBrush(g2))

        p.end()


# ══════════════════════════════════════════════════════════════════════════════
# GLASS CARD
# ══════════════════════════════════════════════════════════════════════════════
class GlassCard(QWidget):
    def __init__(self, parent=None, radius=14):
        super().__init__(parent)
        self.radius = radius
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        r = self.rect()

        # Fill
        p.setBrush(QColor(255, 255, 255, 12))
        p.setPen(Qt.PenStyle.NoPen)
        p.drawRoundedRect(r, self.radius, self.radius)

        # Border
        pen = QPen(QColor(255, 255, 255, 30))
        pen.setWidthF(1.0)
        p.setPen(pen)
        p.setBrush(Qt.BrushStyle.NoBrush)
        p.drawRoundedRect(r.adjusted(1, 1, -1, -1), self.radius, self.radius)

        # Shine on top edge
        shine = QLinearGradient(r.left(), r.top(), r.right(), r.top())
        shine.setColorAt(0.0, QColor(255, 255, 255, 0))
        shine.setColorAt(0.3, QColor(255, 255, 255, 18))
        shine.setColorAt(0.7, QColor(255, 255, 255, 18))
        shine.setColorAt(1.0, QColor(255, 255, 255, 0))
        pen2 = QPen(QBrush(shine), 1.0)
        p.setPen(pen2)
        p.drawLine(r.left() + self.radius, r.top() + 1,
                   r.right() - self.radius, r.top() + 1)
        p.end()


# ══════════════════════════════════════════════════════════════════════════════
# STEP WIDGET
# ══════════════════════════════════════════════════════════════════════════════
class StepWidget(QWidget):
    def __init__(self, num, title, desc, parent=None):
        super().__init__(parent)
        self._state = "idle"
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(12)

        self.num_lbl = QLabel(num)
        self.num_lbl.setFixedSize(28, 28)
        self.num_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.num_lbl)

        info = QVBoxLayout()
        info.setSpacing(1)
        self.title_lbl = QLabel(title)
        self.title_lbl.setStyleSheet(f"color:{C['text']}; font-weight:600; font-size:10px; background:transparent;")
        self.desc_lbl  = QLabel(desc)
        self.desc_lbl.setStyleSheet(f"color:{C['muted']}; font-size:9px; background:transparent;")
        info.addWidget(self.title_lbl)
        info.addWidget(self.desc_lbl)
        layout.addLayout(info)
        layout.addStretch()

        self.set_state("idle")

    def set_state(self, state: str):
        self._state = state
        s = STEP_STATES[state]
        self.num_lbl.setStyleSheet(f"""
            QLabel {{
                color: {s['num_fg']};
                background-color: {s['num_bg']};
                border: 1px solid {s['num_border']};
                border-radius: 6px;
                font-weight: 700;
                font-size: 11px;
            }}
        """)
        self.setStyleSheet(f"background-color: {s['card_bg']}; border-radius: 10px;")

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        s = STEP_STATES[self._state]
        # subtle border
        pen = QPen(QColor(255, 255, 255, 15))
        pen.setWidthF(0.8)
        p.setPen(pen)
        p.setBrush(Qt.BrushStyle.NoBrush)
        p.drawRoundedRect(self.rect().adjusted(1, 1, -1, -1), 10, 10)
        p.end()
        super().paintEvent(event)


# ══════════════════════════════════════════════════════════════════════════════
# STAT CARD
# ══════════════════════════════════════════════════════════════════════════════
class StatCard(QWidget):
    def __init__(self, value, label, color, parent=None):
        super().__init__(parent)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 10, 14, 10)
        layout.setSpacing(2)

        self.val_lbl = QLabel(value)
        self.val_lbl.setStyleSheet(f"color:{color}; font-size:22px; font-weight:700; background:transparent;")
        self.lbl_lbl = QLabel(label)
        self.lbl_lbl.setStyleSheet(f"color:{C['muted']}; font-size:9px; background:transparent;")

        layout.addWidget(self.val_lbl)
        layout.addWidget(self.lbl_lbl)

    def set_value(self, v: str):
        self.val_lbl.setText(v)

    def paintEvent(self, event):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        p.setBrush(QColor(255, 255, 255, 8))
        p.setPen(Qt.PenStyle.NoPen)
        p.drawRoundedRect(self.rect(), 10, 10)
        pen = QPen(QColor(255, 255, 255, 18))
        pen.setWidthF(0.8)
        p.setPen(pen)
        p.setBrush(Qt.BrushStyle.NoBrush)
        p.drawRoundedRect(self.rect().adjusted(1, 1, -1, -1), 10, 10)
        p.end()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN WINDOW
# ══════════════════════════════════════════════════════════════════════════════
class PipelineWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Pipeline - Administrado")
        self.resize(1020, 740)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint)

        self._queue: queue.Queue = queue.Queue()
        self._running = False
        self._drag_pos = QPoint()
        self._step_widgets: dict[str, StepWidget] = {}
        self._stat_cards:   dict[str, StatCard]   = {}

        self._build_ui()
        self._init_steps()

        self._poll_timer = QTimer(self)
        self._poll_timer.timeout.connect(self._poll_queue)
        self._poll_timer.start(100)

    # ── DRAG (frameless window) ──────────────────────────────────────────────
    def mousePressEvent(self, e):
        if e.button() == Qt.MouseButton.LeftButton:
            self._drag_pos = e.globalPosition().toPoint() - self.frameGeometry().topLeft()

    def mouseMoveEvent(self, e):
        if e.buttons() == Qt.MouseButton.LeftButton and not self._drag_pos.isNull():
            self.move(e.globalPosition().toPoint() - self._drag_pos)

    # ── UI BUILD ─────────────────────────────────────────────────────────────
    def _build_ui(self):
        # Background
        self._bg = GradientBackground(self)
        self._bg.setGeometry(self.rect())
        self.resizeEvent = self._on_resize

        # Root container (transparent)
        root = QWidget(self)
        root.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        root.setGeometry(self.rect())
        self.setCentralWidget(root)

        main = QVBoxLayout(root)
        main.setContentsMargins(20, 16, 20, 16)
        main.setSpacing(10)

        main.addWidget(self._build_titlebar())
        main.addWidget(self._build_separator())

        body = QHBoxLayout()
        body.setSpacing(12)
        body.addWidget(self._build_left(),  4)
        body.addWidget(self._build_right(), 6)
        main.addLayout(body, 1)

        main.addWidget(self._build_separator())
        main.addWidget(self._build_footer())

    def _on_resize(self, event):
        self._bg.setGeometry(self.rect())
        if self.centralWidget():
            self.centralWidget().setGeometry(self.rect())

    def _build_separator(self):
        sep = QFrame()
        sep.setFixedHeight(1)
        sep.setStyleSheet("background-color: rgba(255,255,255,20); border:none;")
        return sep

    # ── TITLEBAR ─────────────────────────────────────────────────────────────
    def _build_titlebar(self):
        bar = QWidget()
        bar.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        lay = QHBoxLayout(bar)
        lay.setContentsMargins(0, 0, 0, 0)

        # Logo + title
        logo = QLabel("◈")
        logo.setStyleSheet(f"color:{C['accent']}; font-size:26px; font-weight:700; background:transparent;")
        lay.addWidget(logo)

        titles = QVBoxLayout()
        titles.setSpacing(0)
        t1 = QLabel("Sheet Pipeline")
        t1.setStyleSheet(f"color:{C['text']}; font-size:15px; font-weight:700; background:transparent;")
        t2 = QLabel("Pipeline - Administrado")
        t2.setStyleSheet(f"color:{C['muted']}; font-size:9px; background:transparent;")
        titles.addWidget(t1)
        titles.addWidget(t2)
        lay.addLayout(titles)
        lay.addStretch()

        # Date badge
        dates = get_target_dates()
        if len(dates) == 1:
            badge_text = f"  Processing: {dates[0][6:8]}/{dates[0][4:6]}/{dates[0][:4]}  "
        else:
            badge_text = f"  Processing: {len(dates)} days  ({dates[0][6:8]}/{dates[0][4:6]} – {dates[-1][6:8]}/{dates[-1][4:6]})  "

        badge = QLabel(badge_text)
        badge.setStyleSheet(f"""
            QLabel {{
                color: {C['info']};
                background-color: rgba(124,111,247,28);
                border: 1px solid rgba(124,111,247,60);
                border-radius: 8px;
                font-size: 9px;
                font-weight: 600;
                padding: 5px 4px;
            }}
        """)
        lay.addWidget(badge)

        # Window controls
        for symbol, action, hover in [("─", self.showMinimized, "#3a3a5c"), ("✕", self.close, "#6a2020")]:
            btn = QPushButton(symbol)
            btn.setFixedSize(28, 28)
            btn.setStyleSheet(f"""
                QPushButton {{
                    color: {C['muted']}; background: rgba(255,255,255,8);
                    border: 1px solid rgba(255,255,255,15); border-radius: 6px;
                    font-size: 12px;
                }}
                QPushButton:hover {{ background: {hover}; color: white; }}
            """)
            btn.clicked.connect(action)
            lay.addWidget(btn)

        return bar

    # ── LEFT PANEL ───────────────────────────────────────────────────────────
    def _build_left(self):
        card = GlassCard()
        lay  = QVBoxLayout(card)
        lay.setContentsMargins(14, 14, 14, 14)
        lay.setSpacing(8)

        # Stats
        sec = QLabel("STATUS")
        sec.setStyleSheet(f"color:{C['muted']}; font-size:8px; font-weight:700; letter-spacing:2px; background:transparent;")
        lay.addWidget(sec)

        stats = [
            ("market_files", "0", "Market files matched",  C["accent"]),
            ("market_rows",  "0", "Market rows processed", C["ok"]),
            ("own_rows",     "0", "Own rows processed",    C["warn"]),
            ("uploaded",     "—", "Sheet updated",         C["info"]),
        ]
        for stat_id, val, label, color in stats:
            sc = StatCard(val, label, color)
            lay.addWidget(sc)
            self._stat_cards[stat_id] = sc

        # Divider
        lay.addWidget(self._build_separator())

        # Pipeline steps
        sec2 = QLabel("PIPELINE")
        sec2.setStyleSheet(f"color:{C['muted']}; font-size:8px; font-weight:700; letter-spacing:2px; background:transparent;")
        lay.addWidget(sec2)

        for num, step_id, title, desc in STEPS:
            sw = StepWidget(num, title, desc)
            lay.addWidget(sw)
            self._step_widgets[step_id] = sw

        lay.addStretch()

        # Run button
        self._run_btn = QPushButton("▶   Run Pipeline")
        self._run_btn.setFixedHeight(44)
        self._run_btn.setStyleSheet(f"""
            QPushButton {{
                color: white;
                background-color: {C['accent']};
                border: none;
                border-radius: 10px;
                font-size: 12px;
                font-weight: 700;
            }}
            QPushButton:hover {{
                background-color: {C['accent_hover']};
            }}
            QPushButton:disabled {{
                background-color: #3a3560;
                color: #7070a0;
            }}
        """)
        self._run_btn.clicked.connect(self._start_pipeline)
        lay.addWidget(self._run_btn)

        return card

    # ── RIGHT PANEL ──────────────────────────────────────────────────────────
    def _build_right(self):
        card = GlassCard()
        lay  = QVBoxLayout(card)
        lay.setContentsMargins(14, 14, 14, 14)
        lay.setSpacing(8)

        # Log header
        hdr = QHBoxLayout()
        title = QLabel("LIVE LOG")
        title.setStyleSheet(f"color:{C['muted']}; font-size:8px; font-weight:700; letter-spacing:2px; background:transparent;")
        hdr.addWidget(title)
        hdr.addStretch()

        clear_btn = QPushButton("Clear")
        clear_btn.setFixedSize(52, 22)
        clear_btn.setStyleSheet(f"""
            QPushButton {{
                color: {C['muted']}; background: rgba(255,255,255,8);
                border: 1px solid rgba(255,255,255,15); border-radius: 5px;
                font-size: 9px;
            }}
            QPushButton:hover {{ color: {C['text']}; background: rgba(255,255,255,15); }}
        """)
        clear_btn.clicked.connect(self._clear_log)
        hdr.addWidget(clear_btn)
        lay.addLayout(hdr)

        lay.addWidget(self._build_separator())

        self._log_box = QTextEdit()
        self._log_box.setReadOnly(True)
        self._log_box.setStyleSheet(f"""
            QTextEdit {{
                background: transparent;
                color: {C['text']};
                border: none;
                font-family: Consolas, monospace;
                font-size: 10px;
                selection-background-color: {C['accent']};
            }}
            QScrollBar:vertical {{
                background: rgba(255,255,255,5); width: 6px; border-radius: 3px;
            }}
            QScrollBar::handle:vertical {{
                background: rgba(255,255,255,25); border-radius: 3px;
            }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0; }}
        """)
        lay.addWidget(self._log_box)

        return card

    # ── FOOTER ───────────────────────────────────────────────────────────────
    def _build_footer(self):
        bar = QWidget()
        bar.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        lay = QHBoxLayout(bar)
        lay.setContentsMargins(0, 0, 0, 0)

        self._foot_lbl = QLabel("Ready — press Run to start the pipeline.")
        self._foot_lbl.setStyleSheet(f"color:{C['muted']}; font-size:9px; background:transparent;")
        lay.addWidget(self._foot_lbl)
        lay.addStretch()

        self._open_btn = QPushButton("Open output folder  ↗")
        self._open_btn.setEnabled(False)
        self._open_btn.setStyleSheet(f"""
            QPushButton {{
                color: {C['muted']}; background: transparent;
                border: none; font-size: 9px;
            }}
            QPushButton:enabled {{ color: {C['ok']}; }}
            QPushButton:enabled:hover {{ color: {C['teal']}; text-decoration: underline; }}
        """)
        self._open_btn.clicked.connect(self._open_interm)
        lay.addWidget(self._open_btn)

        return bar

    # ── STEP STATES ──────────────────────────────────────────────────────────
    def _init_steps(self):
        for _, step_id, _, _ in STEPS:
            if not EXTRACT_OWN_ENABLED and step_id in ("extract_own", "transform_own"):
                self._step_widgets[step_id].set_state("skipped")
            else:
                self._step_widgets[step_id].set_state("idle")

    def _set_step(self, step_id: str, state: str):
        sw = self._step_widgets.get(step_id)
        if not sw:
            return
        if sw._state == "skipped" and state != "skipped":
            return
        sw.set_state(state)

    # ── STATS ────────────────────────────────────────────────────────────────
    def _update_stat(self, stat_id: str, value: str):
        sc = self._stat_cards.get(stat_id)
        if sc:
            sc.set_value(value)

    def _reset_stats(self):
        for sid, sc in self._stat_cards.items():
            sc.set_value("—" if sid == "uploaded" else "0")

    # ── LOGGING ──────────────────────────────────────────────────────────────
    def _setup_logging(self):
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        root_logger.handlers.clear()
        log_path = ROOT / PATH_LOGS / "logs.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        root_logger.addHandler(logging.FileHandler(str(log_path), encoding="utf-8"))
        root_logger.addHandler(_UILogHandler(self._queue))

    # ── PIPELINE ─────────────────────────────────────────────────────────────
    def _start_pipeline(self):
        if self._running:
            return
        self._running = True
        self._run_btn.setEnabled(False)
        self._run_btn.setText("⏳  Running...")
        self._init_steps()
        self._reset_stats()
        self._foot_lbl.setText("Pipeline running...")
        self._foot_lbl.setStyleSheet(f"color:{C['warn']}; font-size:9px; background:transparent;")
        self._open_btn.setEnabled(False)
        self._log_box.clear()
        self._setup_logging()
        threading.Thread(target=self._pipeline_thread, daemon=True).start()

    def _pipeline_thread(self):
        try:
            setup_dirs()
            result = run_pipeline()
            self._queue.put(("done", (result.total_rows, result.market_rows, result.own_rows)))
        except Exception as e:
            logging.error(f"Pipeline failed: {e}", exc_info=True)
            self._queue.put(("error", str(e)))

    # ── QUEUE POLL ───────────────────────────────────────────────────────────
    def _poll_queue(self):
        try:
            while True:
                msg_type, payload = self._queue.get_nowait()
                if msg_type == "log":
                    self._handle_log(payload)
                elif msg_type == "done":
                    self._on_done(payload)
                elif msg_type == "error":
                    self._on_error(payload)
        except queue.Empty:
            pass

    def _handle_log(self, record: logging.LogRecord):
        msg = record.getMessage()

        for trigger, transitions in STEP_TRIGGERS:
            if trigger in msg:
                for step_id, state in transitions:
                    self._set_step(step_id, state)

        m = re.search(r'(\d+)/\d+ files match', msg)
        if m:
            self._update_stat("market_files", m.group(1))
        m = re.search(r'Market transform complete: (\d+)', msg)
        if m:
            self._update_stat("market_rows", m.group(1))
        m = re.search(r'Own transform complete: (\d+)', msg)
        if m:
            self._update_stat("own_rows", m.group(1))

        if record.levelno >= logging.ERROR:
            for step_id, sw in self._step_widgets.items():
                if sw._state == "active":
                    self._set_step(step_id, "err")

        if record.levelno >= logging.ERROR:
            color = C["err"]
        elif record.levelno >= logging.WARNING:
            color = C["warn"]
        elif any(k in msg.lower() for k in ("complete", "saved", "done")):
            color = C["ok"]
        else:
            color = C["info"]

        ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        self._log_box.append(
            f'<span style="color:{C["muted"]}">{ts}&nbsp;&nbsp;</span>'
            f'<span style="color:{color}">{msg}</span>'
        )

    def _clear_log(self):
        self._log_box.clear()

    # ── CALLBACKS ────────────────────────────────────────────────────────────
    def _on_done(self, payload):
        total, market_rows, own_rows = payload
        self._running = False
        self._run_btn.setEnabled(True)
        self._run_btn.setText("▶   Run Pipeline")
        self._update_stat("market_rows", str(market_rows))
        self._update_stat("own_rows",    str(own_rows))
        self._update_stat("uploaded",    "✓")
        ts = datetime.now().strftime("%H:%M:%S")
        self._foot_lbl.setText(f"✓  Complete · {total} rows uploaded · {ts}")
        self._foot_lbl.setStyleSheet(f"color:{C['ok']}; font-size:9px; background:transparent;")
        self._open_btn.setEnabled(True)

    def _on_error(self, error_msg: str):
        self._running = False
        self._run_btn.setEnabled(True)
        self._run_btn.setText("▶   Run Pipeline")
        self._foot_lbl.setText(f"✗  Failed: {error_msg[:90]}")
        self._foot_lbl.setStyleSheet(f"color:{C['err']}; font-size:9px; background:transparent;")

    def _open_interm(self):
        path = str(ROOT / PATH_PROCESSED_MARKET)
        if sys.platform == "win32":
            os.startfile(path)
        else:
            import subprocess
            subprocess.Popen(["open" if sys.platform == "darwin" else "xdg-open", path])


# ── ENTRY POINT ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setFont(QFont("Segoe UI", 10))
    win = PipelineWindow()
    win.show()
    sys.exit(app.exec())
