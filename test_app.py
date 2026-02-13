import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox
from typing import Optional, Any, Dict, List, Tuple

import pandas as pd
import seaborn as sns
import matplotlib

matplotlib.use("TkAgg")
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

DB_PATH = "dashboard.db"

THEME = {
    "bg": "#0f172a",
    "card": "#111c33",
    "fg": "#e5e7eb",
    "muted": "#94a3b8",
    "grid": "#233152",
    "palette": ["#38bdf8", "#a78bfa", "#34d399", "#fbbf24", "#fb7185", "#60a5fa"],
    "font": ("Segoe UI", 10),
    "title_font": ("Segoe UI", 16, "bold"),
    "card_title_font": ("Segoe UI", 10, "bold"),
    # button colors
    "btn_primary": "#2563eb",
    "btn_primary_hover": "#1d4ed8",
    "btn_primary_active": "#1e40af",
    "btn_secondary": "#1f2a44",
    "btn_secondary_hover": "#2a3a5f",
    "btn_secondary_active": "#334a7a",
}


# ----------------------------
# Rounded button (Canvas)
# ----------------------------
class RoundedButton(tk.Canvas):
    def __init__(
        self,
        parent,
        text="Button",
        command=None,
        radius=12,
        padding=(14, 8),
        bg="#1f2a44",
        fg="#e5e7eb",
        hover_bg="#2a3a5f",
        active_bg="#334a7a",
        font=("Segoe UI", 10, "bold"),
        width=None,
        canvas_bg=None,   # NEW: optional override
        **kwargs,
    ):
                # ttk parents don't support cget("background"), so pick a safe bg
        if canvas_bg is None:
            try:
                # works for tk widgets (Frame, etc.)
                canvas_bg = parent.cget("bg")
            except Exception:
                # fallback to app background
                canvas_bg = THEME["bg"]
                
        super().__init__(
            parent,
            highlightthickness=0,
            bd=0,
            bg=canvas_bg,
            cursor="hand2",
            **kwargs,
        )
        self.text = text
        self.command = command
        self.radius = radius
        self.padx, self.pady = padding
        self.bg0 = bg
        self.fg = fg
        self.hover_bg = hover_bg
        self.active_bg = active_bg
        self.font = font
        self._state_bg = bg
        self._pressed = False

        # Measure text -> size
        tmp = tk.Label(parent, text=text, font=font)
        tmp.update_idletasks()
        w = (tmp.winfo_reqwidth() + self.padx * 2) if width is None else width
        h = tmp.winfo_reqheight() + self.pady * 2
        tmp.destroy()

        self.configure(width=w, height=h)
        self._draw()

        self.bind("<Enter>", self._on_enter)
        self.bind("<Leave>", self._on_leave)
        self.bind("<ButtonPress-1>", self._on_press)
        self.bind("<ButtonRelease-1>", self._on_release)
        self.bind("<space>", lambda e: self._invoke())
        self.bind("<Return>", lambda e: self._invoke())

        # allow focus via keyboard
        self.configure(takefocus=1)

    def _rounded_rect(self, x1, y1, x2, y2, r, **kwargs):
        points = [
            x1 + r, y1,
            x2 - r, y1,
            x2, y1,
            x2, y1 + r,
            x2, y2 - r,
            x2, y2,
            x2 - r, y2,
            x1 + r, y2,
            x1, y2,
            x1, y2 - r,
            x1, y1 + r,
            x1, y1,
        ]
        return self.create_polygon(points, smooth=True, **kwargs)

    def _draw(self):
        self.delete("all")
        w = int(self["width"])
        h = int(self["height"])

        self._rounded_rect(2, 2, w - 2, h - 2, self.radius, fill=self._state_bg, outline="")
        self.create_text(w // 2, h // 2, text=self.text, fill=self.fg, font=self.font)

        # Focus ring
        if self.focus_displayof() == self:
            self._rounded_rect(1, 1, w - 1, h - 1, self.radius, fill="", outline="#60a5fa", width=2)

    def _invoke(self):
        if self.command:
            self.command()

    def _on_enter(self, _e):
        if not self._pressed:
            self._state_bg = self.hover_bg
            self._draw()

    def _on_leave(self, _e):
        self._pressed = False
        self._state_bg = self.bg0
        self._draw()

    def _on_press(self, _e):
        self._pressed = True
        self._state_bg = self.active_bg
        self.focus_set()
        self._draw()

    def _on_release(self, _e):
        if not self._pressed:
            return
        self._pressed = False
        self._state_bg = self.hover_bg
        self._draw()
        self._invoke()


def PrimaryButton(parent, text, command, **kwargs):
    return RoundedButton(
        parent,
        text=text,
        command=command,
        bg=THEME["btn_primary"],
        hover_bg=THEME["btn_primary_hover"],
        active_bg=THEME["btn_primary_active"],
        **kwargs,
    )


def SecondaryButton(parent, text, command, **kwargs):
    return RoundedButton(parent, text=text, command=command, **kwargs)


# ----------------------------
# DB helpers
# ----------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            month INTEGER NOT NULL CHECK(month BETWEEN 1 AND 12),
            item TEXT NOT NULL,
            value REAL NOT NULL,
            accepted INTEGER NOT NULL DEFAULT 0 CHECK(accepted IN (0,1))
        );
        """
    )
    conn.commit()


def seed_if_empty(conn):
    n = conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"]
    if n:
        return

    cats = ["North", "South", "East", "West"]
    items = [f"Item {i}" for i in range(1, 7)]
    base = {"North": 55, "South": 60, "East": 50, "West": 58}

    rows = []
    for c in cats:
        for m in range(1, 13):
            for i, it in enumerate(items, start=1):
                val = float(base[c] + (m - 6) * 1.1 + i * 2.5)
                rows.append((c, m, it, val, 0))

    conn.executemany(
        "INSERT INTO records (category, month, item, value, accepted) VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()


def list_categories(conn):
    return ["All"] + [
        r["category"]
        for r in conn.execute("SELECT DISTINCT category FROM records ORDER BY category").fetchall()
    ]


def list_months(conn):
    months = [
        int(r["month"])
        for r in conn.execute("SELECT DISTINCT month FROM records ORDER BY month").fetchall()
    ]
    return ["All months"] + [str(m) for m in months]


def df_records(conn, category: str, month_filter: str) -> pd.DataFrame:
    clauses, params = [], []
    if category != "All":
        clauses.append("category = ?")
        params.append(category)
    if month_filter != "All months":
        clauses.append("month = ?")
        params.append(int(month_filter))

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    q = f"SELECT * FROM records {where}"
    return pd.read_sql_query(q, conn, params=params)


def update_record(conn, record_id: int, field: str, value):
    if field not in {"category", "month", "item", "value", "accepted"}:
        raise ValueError("Invalid field")
    conn.execute(f"UPDATE records SET {field}=? WHERE id=?", (value, record_id))
    conn.commit()


def search_record_suggestions(conn, term: str, limit: int = 50) -> List[str]:
    term = (term or "").strip()
    if not term:
        return []
    is_num = term.isdigit()

    if is_num:
        rows = conn.execute(
            "SELECT id, category, item, month FROM records WHERE id = ? ORDER BY id LIMIT ?",
            (int(term), limit),
        ).fetchall()
    else:
        like = f"%{term}%"
        rows = conn.execute(
            """
            SELECT id, category, item, month
            FROM records
            WHERE category LIKE ? OR item LIKE ?
            ORDER BY id
            LIMIT ?
            """,
            (like, like, limit),
        ).fetchall()

    return [f"{r['id']} | {r['category']} | {r['item']} | m={r['month']}" for r in rows]


def parse_suggestion_to_id(s: str) -> Optional[int]:
    if not s:
        return None
    try:
        return int(s.split("|", 1)[0].strip())
    except Exception:
        return None


# ----------------------------
# Styling
# ----------------------------
def apply_ttk_style(root: tk.Tk):
    root.option_add("*Font", THEME["font"])
    root.configure(bg=THEME["bg"])

    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass

    style.configure("TFrame", background=THEME["bg"])
    style.configure("Card.TFrame", background=THEME["card"])
    style.configure("TLabel", background=THEME["bg"], foreground=THEME["fg"])
    style.configure("Muted.TLabel", background=THEME["bg"], foreground=THEME["muted"])
    style.configure(
        "Card.TLabel",
        background=THEME["card"],
        foreground=THEME["fg"],
        font=THEME["card_title_font"],
    )

    style.configure("TCombobox", padding=(6, 6))
    style.configure("TEntry", padding=(6, 6))

    style.configure(
        "Modern.Treeview",
        background=THEME["card"],
        fieldbackground=THEME["card"],
        foreground=THEME["fg"],
        rowheight=28,
        borderwidth=0,
        relief="flat",
    )
    style.configure(
        "Modern.Treeview.Heading",
        background=THEME["bg"],
        foreground=THEME["fg"],
        relief="flat",
        font=("Segoe UI", 10, "bold"),
    )
    style.map("Modern.Treeview", background=[("selected", "#1f2a44")])


def apply_seaborn_theme():
    sns.set_theme(style="whitegrid")
    sns.set_palette(THEME["palette"])


def style_matplotlib_axes(ax):
    ax.set_facecolor(THEME["card"])
    ax.figure.set_facecolor(THEME["card"])
    ax.tick_params(colors=THEME["fg"])
    for spine in ax.spines.values():
        spine.set_color("#2a3a5f")
    ax.title.set_color(THEME["fg"])
    ax.xaxis.label.set_color(THEME["fg"])
    ax.yaxis.label.set_color(THEME["fg"])
    ax.grid(True, linestyle="--", alpha=0.25)
    for line in ax.get_xgridlines() + ax.get_ygridlines():
        line.set_color(THEME["grid"])


# ----------------------------
# Sort + Filter (Excel-ish)
# ----------------------------
FilterSpec = Dict[str, Any]


class TreeviewSortFilter:
    def __init__(self, tree: ttk.Treeview, heading_labels: Optional[Dict[str, str]] = None):
        self.tree = tree
        self.columns = list(tree["columns"])
        if heading_labels is None:
            heading_labels = {c: tree.heading(c).get("text", c) for c in self.columns}
        self.base_heading = dict(heading_labels)

        self.sort_state: Dict[str, bool] = {}
        self._last_sort_col: Optional[str] = None

        self.filters: Dict[str, Optional[FilterSpec]] = {c: None for c in self.columns}
        self._all_rows: List[Tuple] = []
        self._col_types: Dict[str, str] = {}

        tree.bind("<Button-1>", self._on_left_click, add=True)
        tree.bind("<Button-3>", self._on_right_click, add=True)
        tree.bind("<Button-2>", self._on_right_click, add=True)
        tree.bind("<Shift-Button-1>", self._on_shift_left_click, add=True)

        for col in self.columns:
            tree.heading(col, command=lambda c=col: self.toggle_sort(c))

        self._refresh_heading_texts()

    def set_column_types(self, mapping: Dict[str, str]):
        self._col_types.update(mapping)

    def set_data(self, rows: List[Tuple]):
        self._all_rows = list(rows)
        self.apply()

    def clear_all_filters(self):
        for c in self.columns:
            self.filters[c] = None
        self.apply()

    def apply(self):
        rows = self._all_rows

        for col, spec in self.filters.items():
            if spec is None:
                continue
            idx = self._col_index(col)
            if spec["mode"] == "set":
                rows = [r for r in rows if str(r[idx]) in spec["allowed"]]
            else:
                mn = spec.get("min", None)
                mx = spec.get("max", None)
                out = []
                for r in rows:
                    v = self._coerce_num(r[idx])
                    if v is None:
                        continue
                    if mn is not None and v < mn:
                        continue
                    if mx is not None and v > mx:
                        continue
                    out.append(r)
                rows = out

        if self._last_sort_col is not None:
            col = self._last_sort_col
            asc = self.sort_state.get(col, True)
            idx = self._col_index(col)
            if self._col_types.get(col, "str") == "num":
                rows = sorted(
                    rows,
                    key=lambda r: self._coerce_num(r[idx]) if self._coerce_num(r[idx]) is not None else float("inf"),
                    reverse=not asc,
                )
            else:
                rows = sorted(rows, key=lambda r: str(r[idx]).lower(), reverse=not asc)

        self.tree.delete(*self.tree.get_children())
        for r in rows:
            self.tree.insert("", "end", values=r)

        self._refresh_heading_texts()

    def toggle_sort(self, col: str):
        if self._last_sort_col == col:
            self.sort_state[col] = not self.sort_state.get(col, True)
        else:
            self._last_sort_col = col
            self.sort_state[col] = True
        self.apply()

    def _on_left_click(self, event):
        if self.tree.identify_region(event.x, event.y) != "heading":
            return
        col = self._col_from_id(self.tree.identify_column(event.x))
        if col:
            self.toggle_sort(col)
            return "break"

    def _on_shift_left_click(self, event):
        if self.tree.identify_region(event.x, event.y) != "heading":
            return
        col = self._col_from_id(self.tree.identify_column(event.x))
        if col:
            self._open_filter_popup(col, event.x_root, event.y_root)
            return "break"

    def _on_right_click(self, event):
        if self.tree.identify_region(event.x, event.y) != "heading":
            return
        col = self._col_from_id(self.tree.identify_column(event.x))
        if col:
            self._open_filter_popup(col, event.x_root, event.y_root)
            return "break"

    def _open_filter_popup(self, col: str, x_root: int, y_root: int):
        popup = tk.Toplevel(self.tree)
        popup.title(f"Filter: {self.base_heading.get(col, col)}")
        popup.transient(self.tree.winfo_toplevel())
        popup.resizable(False, False)
        popup.geometry(f"+{x_root}+{y_root}")

        frm = ttk.Frame(popup, padding=10)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text=f"Filter '{self.base_heading.get(col, col)}'").grid(
            row=0, column=0, columnspan=3, sticky="w"
        )

        is_num = self._col_types.get(col, "str") == "num"

        search_var = tk.StringVar(value="")
        ttk.Label(frm, text="Search:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ent = ttk.Entry(frm, textvariable=search_var, width=28)
        ent.grid(row=1, column=1, columnspan=2, sticky="ew", pady=(8, 0))
        ent.focus()

        mode_var = tk.StringVar(value="set")
        min_var = tk.StringVar(value="")
        max_var = tk.StringVar(value="")

        if is_num:
            mode_row = ttk.Frame(frm)
            mode_row.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(8, 0))
            ttk.Radiobutton(mode_row, text="Pick values", variable=mode_var, value="set").pack(side="left")
            ttk.Radiobutton(mode_row, text="Range", variable=mode_var, value="range").pack(side="left", padx=(12, 0))

            range_row = ttk.Frame(frm)
            range_row.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(6, 0))
            ttk.Label(range_row, text="Min:").pack(side="left")
            ttk.Entry(range_row, textvariable=min_var, width=10).pack(side="left", padx=(6, 12))
            ttk.Label(range_row, text="Max:").pack(side="left")
            ttk.Entry(range_row, textvariable=max_var, width=10).pack(side="left", padx=(6, 0))
            listbox_row_index = 4
        else:
            listbox_row_index = 2

        lb = tk.Listbox(frm, selectmode="multiple", height=12, width=46)
        lb.grid(row=listbox_row_index, column=0, columnspan=3, sticky="nsew", pady=(8, 0))

        def unique_values_for_col() -> List[str]:
            idx = self._col_index(col)
            base_rows = self._all_rows

            for c, spec in self.filters.items():
                if c == col or spec is None:
                    continue
                j = self._col_index(c)
                if spec["mode"] == "set":
                    base_rows = [r for r in base_rows if str(r[j]) in spec["allowed"]]
                else:
                    mn = spec.get("min", None)
                    mx = spec.get("max", None)
                    tmp = []
                    for r in base_rows:
                        v = self._coerce_num(r[j])
                        if v is None:
                            continue
                        if mn is not None and v < mn:
                            continue
                        if mx is not None and v > mx:
                            continue
                        tmp.append(r)
                    base_rows = tmp

            return sorted({str(r[idx]) for r in base_rows}, key=lambda v: v.lower())

        all_vals = unique_values_for_col()

        def refill_listbox():
            term = search_var.get().strip().lower()
            lb.delete(0, tk.END)
            for v in all_vals:
                if term and term not in v.lower():
                    continue
                lb.insert(tk.END, v)

            current = self.filters.get(col, None)
            if current is None or current.get("mode") != "set":
                for i in range(lb.size()):
                    lb.selection_set(i)
            else:
                allowed = current["allowed"]
                for i in range(lb.size()):
                    if lb.get(i) in allowed:
                        lb.selection_set(i)

        search_var.trace_add("write", lambda *_: refill_listbox())

        def select_all():
            lb.selection_set(0, tk.END)

        def clear_this_filter():
            self.filters[col] = None
            self.apply()
            popup.destroy()

        def apply_filter():
            if is_num and mode_var.get() == "range":
                mn = self._parse_float(min_var.get().strip())
                mx = self._parse_float(max_var.get().strip())
                self.filters[col] = None if (mn is None and mx is None) else {"mode": "range", "min": mn, "max": mx}
            else:
                selected = {lb.get(i) for i in lb.curselection()}
                visible = {lb.get(i) for i in range(lb.size())}
                self.filters[col] = None if (not selected or selected == visible) else {"mode": "set", "allowed": selected}
            self.apply()
            popup.destroy()

        btns = ttk.Frame(frm)
        btns.grid(row=listbox_row_index + 1, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        btns.columnconfigure(0, weight=1)
        btns.columnconfigure(1, weight=1)
        btns.columnconfigure(2, weight=1)

        # Rounded buttons in popups (secondary style)
        SecondaryButton(btns, "Select all", select_all).grid(row=0, column=0, sticky="ew", padx=(0, 6))
        SecondaryButton(btns, "Clear", clear_this_filter).grid(row=0, column=1, sticky="ew", padx=(0, 6))
        PrimaryButton(btns, "Apply", apply_filter).grid(row=0, column=2, sticky="ew")

        refill_listbox()
        popup.bind("<Escape>", lambda e: popup.destroy())

    def _refresh_heading_texts(self):
        filtered_cols = {c for c, spec in self.filters.items() if spec is not None}
        sort_col = self._last_sort_col
        sort_asc = self.sort_state.get(sort_col, True) if sort_col else True

        for c in self.columns:
            label = self.base_heading.get(c, c)
            if c in filtered_cols:
                label += "  ⏷"
            if sort_col == c:
                label += "  ▲" if sort_asc else "  ▼"
            self.tree.heading(c, text=label)

    def _col_from_id(self, col_id: str) -> Optional[str]:
        try:
            i = int(col_id.replace("#", "")) - 1
        except Exception:
            return None
        return self.columns[i] if 0 <= i < len(self.columns) else None

    def _col_index(self, col: str) -> int:
        return self.columns.index(col)

    def _coerce_num(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        s = str(value).strip()
        if s == "":
            return None
        try:
            return float(s.replace(",", ""))
        except Exception:
            return None

    def _parse_float(self, s: str) -> Optional[float]:
        if not s:
            return None
        try:
            return float(s.replace(",", ""))
        except Exception:
            return None


# ----------------------------
# Search slicer (shell row 0, only on Analytics)
# ----------------------------
class RecordSearchSlicer(ttk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent, style="Card.TFrame", padding=10)
        self.app = app
        self.term_var = tk.StringVar(value="")
        self.pick_var = tk.StringVar(value="")

        ttk.Label(self, text="Find record:", style="Card.TLabel").pack(side="left", padx=(0, 10))

        self.entry = ttk.Entry(self, textvariable=self.term_var, width=30)
        self.entry.pack(side="left", padx=(0, 10))

        self.combo = ttk.Combobox(self, textvariable=self.pick_var, values=[], state="readonly", width=60)
        self.combo.pack(side="left", padx=(0, 10), fill="x", expand=True)

        PrimaryButton(self, "Go", self.go_selected).pack(side="left")

        self.term_var.trace_add("write", lambda *_: self._update_suggestions())
        self.combo.bind("<<ComboboxSelected>>", lambda e: self.go_selected())
        self.entry.bind("<Return>", lambda e: self._update_suggestions(force_go_if_single=True))

    def _update_suggestions(self, force_go_if_single: bool = False):
        term = self.term_var.get()
        opts = search_record_suggestions(self.app.conn, term, limit=50)
        self.combo["values"] = opts
        if opts:
            self.pick_var.set(opts[0])
            if force_go_if_single and len(opts) == 1:
                self.go_selected()
        else:
            self.pick_var.set("")

    def go_selected(self):
        rid = parse_suggestion_to_id(self.pick_var.get())
        if rid is None:
            return
        self.app.show("review")
        self.app.review_page.highlight_record(rid)


# ----------------------------
# Edit dialog (uses rounded buttons)
# ----------------------------
class EditRecordDialog(tk.Toplevel):
    def __init__(self, parent, app, record: Dict[str, Any]):
        super().__init__(parent)
        self.app = app
        self.record = record
        self.title(f"Edit Record {record['id']}")
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()

        self.var_category = tk.StringVar(value=str(record["category"]))
        self.var_month = tk.StringVar(value=str(record["month"]))
        self.var_item = tk.StringVar(value=str(record["item"]))
        self.var_value = tk.StringVar(value=str(record["value"]))
        self.var_accepted = tk.StringVar(value="Yes" if int(record["accepted"]) == 1 else "No")

        frm = ttk.Frame(self, padding=14)
        frm.pack(fill="both", expand=True)

        def row(label, widget, r):
            ttk.Label(frm, text=label, style="Muted.TLabel").grid(row=r, column=0, sticky="w", pady=6, padx=(0, 10))
            widget.grid(row=r, column=1, sticky="ew", pady=6)
            frm.columnconfigure(1, weight=1)

        row("Category", ttk.Entry(frm, textvariable=self.var_category), 0)
        row("Month", ttk.Combobox(frm, textvariable=self.var_month, values=[str(i) for i in range(1, 13)], state="readonly"), 1)
        row("Item", ttk.Entry(frm, textvariable=self.var_item), 2)
        row("Value", ttk.Entry(frm, textvariable=self.var_value), 3)
        row("Accepted", ttk.Combobox(frm, textvariable=self.var_accepted, values=["Yes", "No"], state="readonly"), 4)

        btns = ttk.Frame(frm)
        btns.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        btns.columnconfigure(0, weight=1)
        btns.columnconfigure(1, weight=1)

        SecondaryButton(btns, "Cancel", self.destroy).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        PrimaryButton(btns, "Save", self._save).grid(row=0, column=1, sticky="ew")

        self.bind("<Escape>", lambda e: self.destroy())
        self.bind("<Return>", lambda e: self._save())

    def _save(self):
        rid = int(self.record["id"])
        try:
            cat = self.var_category.get().strip()
            if not cat:
                raise ValueError("Category cannot be blank")
            month = int(self.var_month.get())
            if not (1 <= month <= 12):
                raise ValueError("Month must be 1..12")
            item = self.var_item.get().strip()
            if not item:
                raise ValueError("Item cannot be blank")
            value = float(self.var_value.get().strip())
            accepted = 1 if self.var_accepted.get().strip().lower() in {"yes", "y", "1", "true"} else 0

            update_record(self.app.conn, rid, "category", cat)
            update_record(self.app.conn, rid, "month", month)
            update_record(self.app.conn, rid, "item", item)
            update_record(self.app.conn, rid, "value", value)
            update_record(self.app.conn, rid, "accepted", accepted)

        except Exception as e:
            messagebox.showerror("Invalid edit", str(e), parent=self)
            return

        self.destroy()
        self.app.review_page.refresh()
        self.app.analytics_page.refresh()
        self.app.set_status(f"Saved edits for record {rid}")


# ----------------------------
# Pages
# ----------------------------
class AnalyticsPage(ttk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent)
        self.app = app
        self.category_var = tk.StringVar(value="All")
        self.month_var = tk.StringVar(value="All months")
        self._sortfilters: List[TreeviewSortFilter] = []
        self._build()

    def _build(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        header = ttk.Frame(self)
        header.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
        header.columnconfigure(2, weight=1)

        ttk.Label(header, text="Analytics", font=THEME["title_font"]).grid(row=0, column=0, sticky="w")

        slicers = ttk.Frame(header)
        slicers.grid(row=0, column=1, sticky="e", padx=(16, 0))

        ttk.Label(slicers, text="Category:", style="Muted.TLabel").grid(row=0, column=0, padx=(0, 6))
        ttk.OptionMenu(
            slicers, self.category_var, self.category_var.get(), *self.app.category_options, command=lambda _: self.refresh()
        ).grid(row=0, column=1, padx=(0, 12))

        ttk.Label(slicers, text="Month:", style="Muted.TLabel").grid(row=0, column=2, padx=(0, 6))
        self.month_cb = ttk.Combobox(slicers, textvariable=self.month_var, values=self.app.month_options, state="readonly", width=12)
        self.month_cb.grid(row=0, column=3)
        self.month_cb.bind("<<ComboboxSelected>>", lambda e: self.refresh())

        SecondaryButton(header, "Clear table filters", self.clear_all_filters).grid(row=0, column=2, sticky="e")

        main = ttk.Frame(self)
        main.grid(row=1, column=0, sticky="nsew", padx=16, pady=(8, 16))
        main.columnconfigure(0, weight=1)
        main.columnconfigure(1, weight=1)
        main.rowconfigure(0, weight=2)
        main.rowconfigure(1, weight=1)

        charts_card = ttk.Frame(main, style="Card.TFrame", padding=12)
        charts_card.grid(row=0, column=0, columnspan=2, sticky="nsew", pady=(0, 12))
        charts_card.columnconfigure(0, weight=1)
        charts_card.rowconfigure(1, weight=1)
        ttk.Label(charts_card, text="Charts", style="Card.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 8))

        self.fig = Figure(figsize=(10, 3.6), dpi=100)
        self.ax1 = self.fig.add_subplot(1, 2, 1)
        self.ax2 = self.fig.add_subplot(1, 2, 2)
        self.canvas = FigureCanvasTkAgg(self.fig, master=charts_card)
        self.canvas.get_tk_widget().grid(row=1, column=0, sticky="nsew")

        t1_card = ttk.Frame(main, style="Card.TFrame", padding=12)
        t1_card.grid(row=1, column=0, sticky="nsew", padx=(0, 6))
        t1_card.rowconfigure(1, weight=1)
        t1_card.columnconfigure(0, weight=1)
        ttk.Label(t1_card, text="Top Items", style="Card.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 8))

        self.t1_frame = self._make_table(
            t1_card,
            columns=("item", "avg_value", "accepted", "total"),
            headings={"item": "Item", "avg_value": "Avg", "accepted": "Accepted", "total": "Total"},
            numeric_cols={"avg_value", "accepted", "total"},
        )
        self.t1_frame.grid(row=1, column=0, sticky="nsew")

        t2_card = ttk.Frame(main, style="Card.TFrame", padding=12)
        t2_card.grid(row=1, column=1, sticky="nsew", padx=(6, 0))
        t2_card.rowconfigure(1, weight=1)
        t2_card.columnconfigure(0, weight=1)
        ttk.Label(t2_card, text="Acceptance by Category", style="Card.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 8))

        self.t2_frame = self._make_table(
            t2_card,
            columns=("category", "accepted_cnt", "total_cnt", "accepted_pct"),
            headings={"category": "Category", "accepted_cnt": "Accepted", "total_cnt": "Total", "accepted_pct": "%"},
            numeric_cols={"accepted_cnt", "total_cnt", "accepted_pct"},
        )
        self.t2_frame.grid(row=1, column=0, sticky="nsew")

    def _make_table(self, parent, columns, headings: Dict[str, str], numeric_cols: set):
        frame = ttk.Frame(parent)
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)

        tree = ttk.Treeview(frame, columns=columns, show="headings", style="Modern.Treeview", height=7)
        for col in columns:
            tree.heading(col, text=headings.get(col, col))
            tree.column(col, anchor="e" if col in numeric_cols else "w", width=120 if col in numeric_cols else 200)

        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")

        sf = TreeviewSortFilter(tree, heading_labels=headings)
        sf.set_column_types({c: ("num" if c in numeric_cols else "str") for c in columns})
        tree._sortfilter = sf
        self._sortfilters.append(sf)
        return frame

    def clear_all_filters(self):
        for sf in self._sortfilters:
            sf.clear_all_filters()
        self.app.set_status("Cleared table filters (Analytics)")

    def refresh(self):
        cat = self.category_var.get()
        month = self.month_var.get()
        df = df_records(self.app.conn, cat, month)

        self.ax1.clear()
        self.ax2.clear()

        line_df = df.groupby("month", as_index=False)["value"].mean().rename(columns={"value": "avg_value"})
        sns.lineplot(data=line_df, x="month", y="avg_value", marker="o", ax=self.ax1)
        self.ax1.set_title("Trend (Avg Value by Month)")
        self.ax1.set_xlabel("Month")
        self.ax1.set_ylabel("Avg value")
        self.ax1.set_xticks(list(range(1, 13)))

        bar_df = df.groupby("item", as_index=False)["value"].mean().rename(columns={"value": "avg_value"})
        sns.barplot(data=bar_df, x="item", y="avg_value", ax=self.ax2)
        self.ax2.set_title("Breakdown (Avg Value by Item)")
        self.ax2.set_xlabel("Item")
        self.ax2.set_ylabel("Avg value")
        self.ax2.tick_params(axis="x", rotation=25)

        style_matplotlib_axes(self.ax1)
        style_matplotlib_axes(self.ax2)
        self.fig.tight_layout()
        self.canvas.draw()

        top_items = (
            df.groupby("item")
            .agg(avg_value=("value", "mean"), accepted=("accepted", "sum"), total=("id", "count"))
            .sort_values("avg_value", ascending=False)
            .head(8)
            .reset_index()
        )
        t1_rows = [(r.item, f"{r.avg_value:.2f}", str(int(r.accepted)), str(int(r.total))) for r in top_items.itertuples()]
        tree1 = next(w for w in self.t1_frame.winfo_children() if isinstance(w, ttk.Treeview))
        tree1._sortfilter.set_data(t1_rows)

        acc = df.groupby("category").agg(accepted_cnt=("accepted", "sum"), total_cnt=("id", "count")).reset_index()
        acc["accepted_pct"] = (100.0 * acc["accepted_cnt"] / acc["total_cnt"]).round(1)
        t2_rows = [(r.category, str(int(r.accepted_cnt)), str(int(r.total_cnt)), f"{r.accepted_pct:.1f}") for r in acc.itertuples()]
        tree2 = next(w for w in self.t2_frame.winfo_children() if isinstance(w, ttk.Treeview))
        tree2._sortfilter.set_data(t2_rows)

        self.app.set_status(f"Analytics updated — Category={cat}, Month={month}")


class ReviewEditPage(ttk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent)
        self.app = app
        self.category_var = tk.StringVar(value="All")
        self.columns = ("id", "category", "month", "item", "value", "accepted")
        self.sf: Optional[TreeviewSortFilter] = None
        self._build()

    def _build(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        header = ttk.Frame(self)
        header.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
        header.columnconfigure(2, weight=1)

        ttk.Label(header, text="Accept / Edit Records", font=THEME["title_font"]).grid(row=0, column=0, sticky="w")

        controls = ttk.Frame(header)
        controls.grid(row=0, column=1, sticky="e", padx=(16, 0))

        ttk.Label(controls, text="Category:", style="Muted.TLabel").grid(row=0, column=0, padx=(0, 6))
        ttk.OptionMenu(controls, self.category_var, self.category_var.get(), *self.app.category_options, command=lambda _: self.refresh()).grid(
            row=0, column=1, padx=(0, 12)
        )

        SecondaryButton(controls, "Refresh", self.refresh).grid(row=0, column=2, padx=(0, 8))
        PrimaryButton(controls, "Edit selected", self.edit_selected).grid(row=0, column=3, padx=(0, 8))
        SecondaryButton(controls, "Toggle Accept", self.toggle_accept).grid(row=0, column=4)

        SecondaryButton(header, "Clear table filters", self.clear_all_filters).grid(row=0, column=2, sticky="e")

        card = ttk.Frame(self, style="Card.TFrame", padding=12)
        card.grid(row=1, column=0, sticky="nsew", padx=16, pady=(8, 16))
        card.rowconfigure(1, weight=1)
        card.columnconfigure(0, weight=1)

        ttk.Label(card, text="Records (right-click header to filter)", style="Card.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 8))

        frame = ttk.Frame(card)
        frame.grid(row=1, column=0, sticky="nsew")
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(frame, columns=self.columns, show="headings", style="Modern.Treeview")
        headings = {"id": "ID", "category": "Category", "month": "Month", "item": "Item", "value": "Value", "accepted": "Accepted"}

        for col in self.columns:
            self.tree.heading(col, text=headings[col])
            if col in {"id", "month"}:
                self.tree.column(col, width=90, anchor="e")
            elif col == "value":
                self.tree.column(col, width=120, anchor="e")
            elif col == "accepted":
                self.tree.column(col, width=110, anchor="center")
            else:
                self.tree.column(col, width=200, anchor="w")

        vsb = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        self.tree.bind("<Double-1>", lambda e: self.edit_selected())

        self.sf = TreeviewSortFilter(self.tree, heading_labels=headings)
        self.sf.set_column_types({"id": "num", "month": "num", "value": "num"})

    def clear_all_filters(self):
        if self.sf:
            self.sf.clear_all_filters()
            self.app.set_status("Cleared table filters (Review/Edit)")

    def refresh(self):
        df = df_records(self.app.conn, self.category_var.get(), "All months")
        rows = []
        for r in df.itertuples(index=False):
            accepted = "Yes" if int(r.accepted) == 1 else "No"
            rows.append((r.id, r.category, r.month, r.item, f"{r.value:.2f}", accepted))
        assert self.sf is not None
        self.sf.set_data(rows)
        self.app.set_status("Review grid updated")

    def selected_record_id(self) -> Optional[int]:
        sel = self.tree.selection()
        if not sel:
            return None
        vals = self.tree.item(sel[0], "values")
        if not vals:
            return None
        try:
            return int(vals[0])
        except Exception:
            return None

    def get_record(self, rid: int) -> Optional[Dict[str, Any]]:
        row = self.app.conn.execute(
            "SELECT id, category, month, item, value, accepted FROM records WHERE id = ?",
            (rid,),
        ).fetchone()
        return dict(row) if row else None

    def edit_selected(self):
        rid = self.selected_record_id()
        if rid is None:
            messagebox.showinfo("Edit", "Select a row first.")
            return
        rec = self.get_record(rid)
        if rec is None:
            messagebox.showerror("Edit", "Record not found in database.")
            return
        EditRecordDialog(self, self.app, rec)

    def toggle_accept(self):
        sel = self.tree.selection()
        if not sel:
            return
        for iid in sel:
            vals = self.tree.item(iid, "values")
            record_id = int(vals[0])
            new_val = 0 if vals[5] == "Yes" else 1
            update_record(self.app.conn, record_id, "accepted", new_val)

        self.refresh()
        self.app.analytics_page.refresh()

    def highlight_record(self, record_id: int):
        for iid in self.tree.get_children():
            vals = self.tree.item(iid, "values")
            if vals and int(vals[0]) == int(record_id):
                self.tree.selection_set(iid)
                self.tree.focus(iid)
                self.tree.see(iid)
                self.app.set_status(f"Selected record {record_id}")
                return

        if self.sf:
            self.sf.clear_all_filters()
        self.refresh()

        for iid in self.tree.get_children():
            vals = self.tree.item(iid, "values")
            if vals and int(vals[0]) == int(record_id):
                self.tree.selection_set(iid)
                self.tree.focus(iid)
                self.tree.see(iid)
                self.app.set_status(f"Selected record {record_id} (filters cleared)")
                return

        self.app.set_status(f"Record {record_id} not found")


# ----------------------------
# App shell
# ----------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Seaborn Dashboard (Rounded Buttons)")
        self.geometry("1400x860")

        apply_ttk_style(self)
        apply_seaborn_theme()

        self.conn = get_conn()
        init_db(self.conn)
        seed_if_empty(self.conn)

        self.category_options = list_categories(self.conn)
        self.month_options = list_months(self.conn)

        # root grid
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

        shell = ttk.Frame(self)
        shell.grid(row=0, column=0, sticky="nsew")
        shell.rowconfigure(1, weight=1)
        shell.columnconfigure(0, weight=1)

        # Search slicer row (shown only on analytics)
        self.search_slicer = RecordSearchSlicer(shell, self)
        self.search_slicer.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 0))

        main = ttk.Frame(shell)
        main.grid(row=1, column=0, sticky="nsew", padx=16, pady=16)
        main.rowconfigure(0, weight=1)
        main.columnconfigure(1, weight=1)

        self.nav = ttk.Frame(main, style="Card.TFrame", padding=12)
        self.nav.grid(row=0, column=0, sticky="ns", padx=(0, 12))

        self.content = ttk.Frame(main)
        self.content.grid(row=0, column=1, sticky="nsew")
        self.content.rowconfigure(0, weight=1)
        self.content.columnconfigure(0, weight=1)

        ttk.Label(self.nav, text="Navigation", style="Card.TLabel").pack(anchor="w", pady=(0, 10))

        # Rounded nav buttons
        SecondaryButton(self.nav, "Analytics", lambda: self.show("analytics"), width=180).pack(fill="x", pady=6)
        SecondaryButton(self.nav, "Review / Edit", lambda: self.show("review"), width=180).pack(fill="x", pady=6)
        SecondaryButton(self.nav, "Exit", self.on_close, width=180).pack(fill="x", pady=(18, 0))

        self.analytics_page = AnalyticsPage(self.content, self)
        self.review_page = ReviewEditPage(self.content, self)

        for p in (self.analytics_page, self.review_page):
            p.grid(row=0, column=0, sticky="nsew")

        # status row
        self.status = tk.StringVar(value="Ready")
        status_lbl = ttk.Label(self, textvariable=self.status, padding=8, anchor="w")
        status_lbl.grid(row=1, column=0, sticky="ew")
        self.rowconfigure(1, weight=0)

        self.show("analytics")
        self.analytics_page.refresh()
        self.review_page.refresh()

        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def show(self, name: str):
        if name == "analytics":
            self.search_slicer.grid()  # show
            self.analytics_page.tkraise()
            self.set_status("Analytics")
        else:
            self.search_slicer.grid_remove()  # hide
            self.review_page.tkraise()
            self.set_status("Review / Edit")

    def set_status(self, text: str):
        self.status.set(text)

    def on_close(self):
        try:
            self.conn.close()
        except Exception:
            pass
        self.destroy()


if __name__ == "__main__":
    App().mainloop()
