import os
import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox
from dataclasses import dataclass
from typing import Optional

import matplotlib
matplotlib.use("TkAgg")
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

DB_PATH = "dashboard.db"

# ----------------------------
# SQLite helpers
# ----------------------------
def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_records_cat_month ON records(category, month);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_records_cat_item ON records(category, item);")
    conn.commit()


def seed_db_if_empty(conn: sqlite3.Connection) -> None:
    cur = conn.execute("SELECT COUNT(*) AS n FROM records;")
    n = cur.fetchone()["n"]
    if n > 0:
        return

    # Seed with deterministic-ish sample data
    categories = ["North", "South", "East", "West"]
    items = [f"Item {i}" for i in range(1, 7)]  # 6 items for bar chart

    rows = []
    base = {"North": 55, "South": 60, "East": 50, "West": 58}
    for cat in categories:
        for m in range(1, 13):
            # One record per month per item (so the grid has lots of rows; charts aggregate)
            for i, item in enumerate(items, start=1):
                val = float(base[cat] + (m - 6) * 1.1 + i * 2.5)
                rows.append((cat, m, item, val, 0))

    conn.executemany(
        "INSERT INTO records (category, month, item, value, accepted) VALUES (?, ?, ?, ?, ?);",
        rows
    )
    conn.commit()


def fetch_records(conn: sqlite3.Connection, category: str) -> list[sqlite3.Row]:
    if category == "All":
        cur = conn.execute(
            "SELECT * FROM records ORDER BY category, month, item, id;"
        )
    else:
        cur = conn.execute(
            "SELECT * FROM records WHERE category = ? ORDER BY month, item, id;",
            (category,)
        )
    return cur.fetchall()


def update_record(conn: sqlite3.Connection, record_id: int, field: str, value) -> None:
    if field not in {"category", "month", "item", "value", "accepted"}:
        raise ValueError("Invalid field")
    conn.execute(f"UPDATE records SET {field} = ? WHERE id = ?;", (value, record_id))
    conn.commit()


def aggregate_line(conn: sqlite3.Connection, category: str) -> tuple[list[int], list[float]]:
    # Trend: average value by month
    if category == "All":
        cur = conn.execute(
            """
            SELECT month, AVG(value) AS v
            FROM records
            GROUP BY month
            ORDER BY month;
            """
        )
    else:
        cur = conn.execute(
            """
            SELECT month, AVG(value) AS v
            FROM records
            WHERE category = ?
            GROUP BY month
            ORDER BY month;
            """,
            (category,)
        )
    rows = cur.fetchall()
    months = [r["month"] for r in rows]
    vals = [float(r["v"]) for r in rows]
    return months, vals


def aggregate_bars(conn: sqlite3.Connection, category: str) -> tuple[list[str], list[float]]:
    # Breakdown: average value by item
    if category == "All":
        cur = conn.execute(
            """
            SELECT item, AVG(value) AS v
            FROM records
            GROUP BY item
            ORDER BY item;
            """
        )
    else:
        cur = conn.execute(
            """
            SELECT item, AVG(value) AS v
            FROM records
            WHERE category = ?
            GROUP BY item
            ORDER BY item;
            """,
            (category,)
        )
    rows = cur.fetchall()
    labels = [r["item"] for r in rows]
    vals = [float(r["v"]) for r in rows]
    return labels, vals


# ----------------------------
# Tkinter app
# ----------------------------
class DashboardApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Tkinter Dashboard + Editable Grid + SQLite")
        self.geometry("1250x760")

        self.conn = get_conn()
        init_db(self.conn)
        seed_db_if_empty(self.conn)

        self.categories = ["All"] + self._load_categories()
        self.selected_category = tk.StringVar(value=self.categories[0])

        self.status = tk.StringVar(value="Ready")

        # Treeview edit state
        self._edit_entry: Optional[ttk.Entry] = None
        self._edit_item_id: Optional[str] = None
        self._edit_col: Optional[str] = None

        self._build_ui()
        self.refresh_all()

        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def _load_categories(self) -> list[str]:
        cur = self.conn.execute("SELECT DISTINCT category FROM records ORDER BY category;")
        return [r["category"] for r in cur.fetchall()]

    def _build_ui(self):
        # Controls
        controls = ttk.Frame(self, padding=10)
        controls.pack(side=tk.TOP, fill=tk.X)

        ttk.Label(controls, text="Category:").pack(side=tk.LEFT)
        ttk.OptionMenu(
            controls,
            self.selected_category,
            self.selected_category.get(),
            *self.categories,
            command=lambda _: self.refresh_all()
        ).pack(side=tk.LEFT, padx=8)

        ttk.Button(controls, text="Refresh", command=self.refresh_all).pack(side=tk.LEFT, padx=8)
        ttk.Button(controls, text="Add Row", command=self.add_row).pack(side=tk.LEFT, padx=8)
        ttk.Button(controls, text="Delete Selected", command=self.delete_selected).pack(side=tk.LEFT, padx=8)
        ttk.Button(controls, text="Toggle Accept (Selected)", command=self.toggle_accept_selected).pack(side=tk.LEFT, padx=8)

        ttk.Separator(self, orient="horizontal").pack(fill=tk.X)

        # Layout: charts on top, grid below
        main = ttk.Frame(self, padding=10)
        main.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        main.rowconfigure(0, weight=1)
        main.rowconfigure(1, weight=2)
        main.columnconfigure(0, weight=1)
        main.columnconfigure(1, weight=1)

        # Chart 1
        self.fig1 = Figure(figsize=(5, 3.6), dpi=100)
        self.ax1 = self.fig1.add_subplot(111)
        self.canvas1 = FigureCanvasTkAgg(self.fig1, master=main)
        self.canvas1.get_tk_widget().grid(row=0, column=0, sticky="nsew", padx=(0, 10), pady=(0, 10))

        # Chart 2
        self.fig2 = Figure(figsize=(5, 3.6), dpi=100)
        self.ax2 = self.fig2.add_subplot(111)
        self.canvas2 = FigureCanvasTkAgg(self.fig2, master=main)
        self.canvas2.get_tk_widget().grid(row=0, column=1, sticky="nsew", pady=(0, 10))

        # Grid frame
        grid_frame = ttk.Frame(main)
        grid_frame.grid(row=1, column=0, columnspan=2, sticky="nsew")
        grid_frame.rowconfigure(0, weight=1)
        grid_frame.columnconfigure(0, weight=1)

        # Treeview + scrollbars
        self.columns = ("id", "category", "month", "item", "value", "accepted")
        self.tree = ttk.Treeview(grid_frame, columns=self.columns, show="headings", height=16)
        for col in self.columns:
            self.tree.heading(col, text=col)
            # sensible widths
            if col == "id":
                self.tree.column(col, width=70, anchor="center")
            elif col in ("month", "accepted"):
                self.tree.column(col, width=90, anchor="center")
            elif col == "value":
                self.tree.column(col, width=120, anchor="e")
            else:
                self.tree.column(col, width=180, anchor="w")

        vsb = ttk.Scrollbar(grid_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(grid_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        # Bind double-click for editing
        self.tree.bind("<Double-1>", self.on_double_click)

        # Status bar
        status_bar = ttk.Label(self, textvariable=self.status, padding=6, anchor="w")
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    # ----------------------------
    # Refresh
    # ----------------------------
    def refresh_all(self):
        self.refresh_grid()
        self.refresh_charts()
        self.status.set(f"Showing: {self.selected_category.get()}")

    def refresh_grid(self):
        # clear
        for item in self.tree.get_children():
            self.tree.delete(item)

        cat = self.selected_category.get()
        rows = fetch_records(self.conn, cat)
        for r in rows:
            accepted = "Yes" if int(r["accepted"]) == 1 else "No"
            self.tree.insert("", "end", values=(r["id"], r["category"], r["month"], r["item"], f"{r['value']:.2f}", accepted))

    def refresh_charts(self):
        cat = self.selected_category.get()

        months, line_vals = aggregate_line(self.conn, cat)
        labels, bar_vals = aggregate_bars(self.conn, cat)

        # line
        self.ax1.clear()
        self.ax1.plot(months, line_vals, marker="o")
        self.ax1.set_title(f"Trend (Avg Value by Month) — {cat}")
        self.ax1.set_xlabel("Month")
        self.ax1.set_ylabel("Avg value")
        self.ax1.set_xticks(list(range(1, 13)))
        self.ax1.grid(True, linestyle="--", alpha=0.3)
        self.canvas1.draw()

        # bar
        self.ax2.clear()
        self.ax2.bar(labels, bar_vals)
        self.ax2.set_title(f"Breakdown (Avg Value by Item) — {cat}")
        self.ax2.set_xlabel("Item")
        self.ax2.set_ylabel("Avg value")
        self.ax2.tick_params(axis="x", rotation=25)
        self.canvas2.draw()

    # ----------------------------
    # CRUD actions
    # ----------------------------
    def add_row(self):
        # Add a simple default row for the currently selected category (or first real category if All)
        cat = self.selected_category.get()
        if cat == "All":
            # choose a real category
            cat = self.categories[1] if len(self.categories) > 1 else "North"

        try:
            self.conn.execute(
                "INSERT INTO records (category, month, item, value, accepted) VALUES (?, ?, ?, ?, ?);",
                (cat, 1, "Item 1", 0.0, 0)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            messagebox.showerror("DB Error", str(e))
            return

        self.refresh_all()

    def delete_selected(self):
        sel = self.tree.selection()
        if not sel:
            return

        if not messagebox.askyesno("Confirm", f"Delete {len(sel)} selected row(s)?"):
            return

        try:
            for iid in sel:
                record_id = int(self.tree.item(iid, "values")[0])
                self.conn.execute("DELETE FROM records WHERE id = ?;", (record_id,))
            self.conn.commit()
        except sqlite3.Error as e:
            messagebox.showerror("DB Error", str(e))
            return

        self.refresh_all()

    def toggle_accept_selected(self):
        sel = self.tree.selection()
        if not sel:
            return
        try:
            for iid in sel:
                vals = self.tree.item(iid, "values")
                record_id = int(vals[0])
                accepted_str = vals[5]
                new_val = 0 if accepted_str == "Yes" else 1
                update_record(self.conn, record_id, "accepted", new_val)
        except Exception as e:
            messagebox.showerror("Error", str(e))
            return

        self.refresh_all()

    # ----------------------------
    # Editable cell behavior
    # ----------------------------
    def on_double_click(self, event):
        # Identify clicked row/column
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return

        row_id = self.tree.identify_row(event.y)
        col_id = self.tree.identify_column(event.x)  # like '#1'
        if not row_id or not col_id:
            return

        col_index = int(col_id.replace("#", "")) - 1
        col_name = self.columns[col_index]

        # Disallow editing ID in-place
        if col_name == "id":
            return

        # Get bbox for placement
        bbox = self.tree.bbox(row_id, col_id)
        if not bbox:
            return
        x, y, w, h = bbox

        # Clean up prior editor
        self._destroy_editor()

        # Current value
        current = self.tree.set(row_id, col_name)

        # Use an Entry editor
        self._edit_entry = ttk.Entry(self.tree)
        self._edit_entry.insert(0, current)
        self._edit_entry.select_range(0, tk.END)
        self._edit_entry.focus()

        self._edit_item_id = row_id
        self._edit_col = col_name

        self._edit_entry.place(x=x, y=y, width=w, height=h)

        self._edit_entry.bind("<Return>", self.commit_edit)
        self._edit_entry.bind("<Escape>", lambda e: self._destroy_editor())
        self._edit_entry.bind("<FocusOut>", self.commit_edit)

    def commit_edit(self, event=None):
        if not self._edit_entry or not self._edit_item_id or not self._edit_col:
            return

        new_text = self._edit_entry.get().strip()
        item_id = self._edit_item_id
        col = self._edit_col

        values = list(self.tree.item(item_id, "values"))
        record_id = int(values[0])

        try:
            if col == "month":
                m = int(new_text)
                if not (1 <= m <= 12):
                    raise ValueError("month must be 1..12")
                update_record(self.conn, record_id, "month", m)
                self.tree.set(item_id, "month", str(m))

            elif col == "value":
                v = float(new_text)
                update_record(self.conn, record_id, "value", v)
                self.tree.set(item_id, "value", f"{v:.2f}")

            elif col == "accepted":
                # allow typing: yes/no/1/0
                t = new_text.lower()
                if t in {"yes", "y", "1", "true"}:
                    a = 1
                    show = "Yes"
                elif t in {"no", "n", "0", "false"}:
                    a = 0
                    show = "No"
                else:
                    raise ValueError("accepted must be Yes/No (or 1/0)")
                update_record(self.conn, record_id, "accepted", a)
                self.tree.set(item_id, "accepted", show)

            elif col == "category":
                if not new_text:
                    raise ValueError("category cannot be blank")
                update_record(self.conn, record_id, "category", new_text)
                self.tree.set(item_id, "category", new_text)

            elif col == "item":
                if not new_text:
                    raise ValueError("item cannot be blank")
                update_record(self.conn, record_id, "item", new_text)
                self.tree.set(item_id, "item", new_text)

            else:
                raise ValueError("Unsupported column edit")

        except Exception as e:
            messagebox.showerror("Invalid Edit", str(e))
        finally:
            self._destroy_editor()
            # charts may change when values/categories change
            self.refresh_charts()

    def _destroy_editor(self):
        if self._edit_entry is not None:
            try:
                self._edit_entry.destroy()
            except Exception:
                pass
        self._edit_entry = None
        self._edit_item_id = None
        self._edit_col = None

    def on_close(self):
        try:
            self._destroy_editor()
            self.conn.close()
        except Exception:
            pass
        self.destroy()


if __name__ == "__main__":
    app = DashboardApp()
    app.mainloop()
