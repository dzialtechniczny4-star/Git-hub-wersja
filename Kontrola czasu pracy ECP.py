import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import messagebox, simpledialog, StringVar
from datetime import timedelta, datetime, date
import mysql.connector
import tkinter as tk
from tkinter import ttk
from calendar import month_name
import locale
import csv
from tkinter import filedialog
import openpyxl
from openpyxl.styles import Font
import pandas as pd
import json
import tkinter.font as tkfont
import json
import tempfile
from pathlib import Path
import os, sys, urllib.request, threading, queue, subprocess
from tkinter import Tk, Toplevel, Label, StringVar, messagebox
from ttkbootstrap import Style
from ttkbootstrap.widgets import Progressbar

CURRENT_VERSION = "5"
VERSION_URL     = "https://github.com/dzialtechniczny4-star/Git-hub-wersja/raw/refs/heads/main/Kontrola_czasu_pracy_ECP.exe"
TIMEOUT         = 5 

# ---------------------   POBIERANIE  -------------------------

def read_remote_version():
    with urllib.request.urlopen(VERSION_URL, timeout=TIMEOUT) as r:
        text = r.read().decode("utf-8").strip().splitlines()
    return text[0].strip(), (text[1].strip() if len(text) > 1 else None)

def is_newer(remote, local):
    t = lambda v: tuple(map(int, v.split(".")))
    return t(remote) > t(local)

def download_file(url:str, dest:Path, q:queue.Queue):
    """Pobiera url do dest, co ~chunk wrzuca % do kolejki q."""
    try:
        with urllib.request.urlopen(url) as resp, open(dest, "wb") as out:
            total = int(resp.getheader("Content-Length", "0"))
            downloaded, chunk = 0, 8192
            while True:
                data = resp.read(chunk)
                if not data:
                    break
                out.write(data)
                downloaded += len(data)
                if total:
                    q.put(downloaded / total * 100)
        q.put("done")
    except Exception as e:
        q.put(("error", str(e)))

# ---------------------   GUI   --------------------------------

def show_update_window(remote_ver:str, exe_url:str):
    dest_dir  = Path(sys.executable).resolve().parent
    base_name = Path(exe_url).stem
    new_name  = f"{base_name}-{remote_ver}.exe"
    dest_path = dest_dir / new_name

    q = queue.Queue()
    t = threading.Thread(target=download_file, args=(exe_url, dest_path, q), daemon=True)
    t.start()

    # --- małe okno modalne ---
    root = Tk()
    root.withdraw()                   # główne niepotrzebne
    win  = Toplevel()
    win.title("Aktualizacja")
    Style("flatly")                   # ładny bootstrapowy styl

    Label(win, text=f"Nowa wersja {remote_ver} – trwa pobieranie").pack(padx=18, pady=(12, 6))
    p_var = StringVar(value="0 %")
    bar   = Progressbar(win, length=320, variable=p_var, maximum=100, bootstyle="success-striped")
    bar.pack(padx=18, pady=(0, 8))

    def poll_queue():
        try:
            while True:
                msg = q.get_nowait()
                if msg == "done":
                    bar["value"] = 100
                    p_var.set("100 %")
                    win.update()
                    messagebox.showinfo("Aktualizacja", "Pobrano – uruchamiam nową wersję.")
                    launch_new_exe(str(dest_path))
                elif isinstance(msg, tuple) and msg[0] == "error":
                    messagebox.showerror("Aktualizacja", f"Błąd pobierania:\n{msg[1]}")
                    win.destroy()
                else:                       # liczba %
                    bar["value"] = msg
                    p_var.set(f"{msg:.0f} %")
        except queue.Empty:
            pass
        win.after(200, poll_queue)

    poll_queue()
    win.protocol("WM_DELETE_WINDOW", lambda: None)  # blokuj zamknięcie
    win.mainloop()

# ---------------------   START/UPDATE  ------------------------

def launch_new_exe(exe_path):
    subprocess.Popen([exe_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    sys.exit(0)

def remove_old_versions(my_path:Path):
    stem = my_path.stem.split("-")[0]
    for f in my_path.parent.glob(f"{stem}-*.exe"):
        if f != my_path:
            try: f.unlink()
            except PermissionError: pass

def check_for_update():
    try:
        remote_ver, exe_url = read_remote_version()
        if is_newer(remote_ver, CURRENT_VERSION) and exe_url:
            show_update_window(remote_ver, exe_url)   # ← przejmuje sterowanie
        else:
            print("Aktualna wersja:", CURRENT_VERSION)
    except Exception as e:
        print("Nie udało się sprawdzić aktualizacji:", e)

# --- KONFIGURACJA BAZY ---
MYSQL_CONFIG = {
    'host':     '10.41.5.40',
    'user':     'dt',
    'password': 'P8PAs!h$@*auVO0l',
    'database': 'kontrola_czasu_pracy_dt'
}
def center_popup(win, parent):
    win.update_idletasks()
    parent_x = parent.winfo_rootx()
    parent_y = parent.winfo_rooty()
    parent_w = parent.winfo_width()
    parent_h = parent.winfo_height()
    w = win.winfo_width()
    h = win.winfo_height()
    x = parent_x + (parent_w // 2) - (w // 2)
    y = parent_y + (parent_h // 2) - (h // 2)
    win.geometry(f"+{x}+{y}")

def connect_db():
    return mysql.connector.connect(**MYSQL_CONFIG)

def save_dynamic_procenty(miesiac, columns, rows):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM procenty_miesiac WHERE miesiac=%s", (miesiac,))
    data = json.dumps({"columns": columns, "rows": rows}, ensure_ascii=False)
    cur.execute("INSERT INTO procenty_miesiac (miesiac, dane_json) VALUES (%s, %s)", (miesiac, data))
    conn.commit()
    conn.close()

def load_dynamic_procenty(miesiac):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT dane_json FROM procenty_miesiac WHERE miesiac = %s", (miesiac,))
    res = cur.fetchone()
    conn.close()
    if res and res[0]:
        data = json.loads(res[0])
        return data["columns"], data["rows"]
    else:
        return [], []


def insert_record(row):
    conn = connect_db()
    cur = conn.cursor()
    sql = """
    INSERT INTO raport_ecp (data, osoba, kraj, zadanie, opis, ilosc, czas_od, czas_do)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(sql, row)
    conn.commit()
    conn.close()
    

def get_zmiana_na_dzien(username, data):  # szuka zmianę na dany dzień (DD.MM.YYYY)
    conn = connect_db()
    cur = conn.cursor()
    try:
        data_mysql = datetime.strptime(data, "%d.%m.%Y").strftime("%Y-%m-%d")
    except Exception:
        data_mysql = datetime.now().strftime("%Y-%m-%d")
    cur.execute("SELECT id, czas_od, czas_do FROM raport_zmiany WHERE osoba=%s AND data=%s ORDER BY id DESC", (username, data_mysql))
    row = cur.fetchone()
    conn.close()
    return row  # None albo (id, czas_od, czas_do)

def rozpocznij_zmiane(username, data):
    conn = connect_db()
    cur = conn.cursor()
    data_mysql = datetime.strptime(data, "%d.%m.%Y").strftime("%Y-%m-%d")
    now = datetime.now().strftime("%H:%M:%S")
    cur.execute("INSERT INTO raport_zmiany (osoba, data, czas_od) VALUES (%s, %s, %s)", (username, data_mysql, now))
    conn.commit()
    conn.close()
    return now

def sumuj_czasy_td(lista_par_czasow):
    total = timedelta()
    now = datetime.now().replace(microsecond=0)
    dzis = datetime.now().date()
    for od, do in lista_par_czasow:
        if not od or od in ("None", "null", "NULL", ""):
            continue
        try:
            t1 = datetime.strptime(od, "%H:%M:%S")
            # Jeśli brak końca, a zadanie z dziś — użyj aktualnej godziny:
            if not do or do in ("None", "null", "NULL", ""):
                t2 = now
            else:
                t2 = datetime.strptime(do, "%H:%M:%S")
                if t2 < t1:
                    t2 += timedelta(days=1)
            diff = t2 - t1
            total += diff
        except Exception:
            continue
    return total

def sumuj_czasy(lista_par_czasow):
    total = timedelta()
    for od, do in lista_par_czasow:
        if not od or not do or od in ("None", "null", "NULL", "") or do in ("None", "null", "NULL", ""):
            continue
        try:
            t1 = datetime.strptime(od, "%H:%M:%S")
            t2 = datetime.strptime(do, "%H:%M:%S")
            diff = t2 - t1
            if diff.total_seconds() < 0:
                diff += timedelta(days=1)
            total += diff
        except Exception:
            continue
    hours = total.days * 24 + total.seconds // 3600
    minutes = (total.seconds % 3600) // 60
    seconds = total.seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def show_db_error_popup(parent, refresh_callback):
    popup = tk.Toplevel(parent)
    popup.title("Błąd połączenia z bazą")
    popup.geometry("380x160")
    popup.transient(parent)
    popup.grab_set()
    tk.Label(
        popup, text="Utracono połączenie z bazą danych.\nKliknij 'Odśwież', aby spróbować ponownie.",
        font=("Segoe UI", 11), wraplength=340, justify="center"
    ).pack(pady=(22, 12))
    btn = tk.Button(popup, text="Odśwież", font=("Segoe UI", 11, "bold"), width=18, command=lambda: (popup.destroy(), refresh_callback()))
    btn.pack(pady=(10, 18))
    popup.bind("<Return>", lambda e: (popup.destroy(), refresh_callback()))
    popup.focus_set()
    popup.wait_window()

def fetch_all_records(user=None, parent=None, refresh_callback=None):
    try:
        conn = connect_db()
        cur = conn.cursor()
        if user:
            cur.execute("SELECT id, data, osoba, kraj, zadanie, opis, ilosc, czas_od, czas_do FROM raport_ecp WHERE osoba=%s ORDER BY id DESC", (user,))
        else:
            cur.execute("SELECT id, data, osoba, kraj, zadanie, opis, ilosc, czas_od, czas_do FROM raport_ecp ORDER BY id DESC")
        records = cur.fetchall()
        conn.close()
        return records
    except Exception as e:
        print("Błąd pobierania rekordów:", e)
        if parent and refresh_callback:
            show_db_error_popup(parent, refresh_callback)
        return []
    
def update_czas_do(row_id, czas_do):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("UPDATE raport_ecp SET czas_do=%s WHERE id=%s", (czas_do, row_id))
    conn.commit()
    conn.close()

def fetch_nazwiska():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT nazwisko_imie FROM nazwiska ORDER BY nazwisko_imie")
    rows = cur.fetchall()
    conn.close()
    return [row[0] for row in rows]

def add_nazwisko_db(nazwisko_imie):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("INSERT IGNORE INTO nazwiska (nazwisko_imie) VALUES (%s)", (nazwisko_imie,))
    conn.commit()
    conn.close()

def fetch_kraje():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT nazwa FROM kraje ORDER BY nazwa")
    rows = cur.fetchall()
    conn.close()
    return [row[0] for row in rows]

def fetch_zadania(kraj):
    if not kraj: return []
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT zadanie FROM zadania WHERE kraj=%s ORDER BY zadanie", (kraj,))
    rows = cur.fetchall()
    conn.close()
    return [row[0] for row in rows]

def add_kraj_db(nazwa):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("INSERT IGNORE INTO kraje (nazwa) VALUES (%s)", (nazwa,))
    conn.commit()
    conn.close()

def load_users():
    conn = connect_db()
    with conn.cursor(dictionary=True) as cur:
        cur.execute("SELECT login, password, role FROM user")
        users = {
            row["login"]: {"password": row["password"], "role": row["role"]}
            for row in cur.fetchall()
        }
    conn.close()
    return users

def load_users_name():
    conn = connect_db()
    with conn.cursor(dictionary=True) as cur:
        cur.execute("SELECT login, name FROM user")
        users = {
            row["login"]:  row["name"]
            for row in cur.fetchall()
        }
    conn.close()
    return users

USERS = load_users() 
USER_TO_NAME = load_users_name() 

def get_real_start_end_time(username, data):
    conn = connect_db()
    cur = conn.cursor()
    try:
        data_mysql = datetime.strptime(data, "%d.%m.%Y").strftime("%Y-%m-%d")
    except Exception:
        data_mysql = datetime.now().strftime("%Y-%m-%d")
    cur.execute("""
        SELECT MIN(czas_od), MAX(czas_do)
        FROM raport_zmiany
        WHERE osoba=%s AND data=%s
    """, (username, data_mysql))
    row = cur.fetchone()
    conn.close()
    return row[0] or "", row[1] or ""

# --- CZASY DEKLAROWANE ---
def load_czasy_deklarowane():
    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT kraj, zadanie, IFNULL(czas_deklarowany,'') FROM zadania")
        slownik = {}
        for kraj, zad, czas in cur.fetchall():
            slownik.setdefault(zad, {})[kraj] = czas
    conn.close()
    return slownik
CZASY_DEKLAROWANE = load_czasy_deklarowane()
    
def seconds_to_hms(seconds):
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def time_diff(od, do):
    if not od or not do:
        return ""
    try:
        fmt = "%H:%M:%S"
        t1 = datetime.strptime(str(od), fmt)
        t2 = datetime.strptime(str(do), fmt)
        delta = (t2 - t1)
        total_seconds = int(delta.total_seconds())
        if total_seconds < 0:  # przekroczenie północy
            total_seconds += 24 * 3600
        h = total_seconds // 3600
        m = (total_seconds % 3600) // 60
        s = total_seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"
    except Exception:
        return ""

def hms_to_seconds(hms):
    if not hms:
        return 0
    parts = [int(float(x)) for x in hms.split(":")]
    while len(parts) < 3:
        parts.append(0)
    return parts[0]*3600 + parts[1]*60 + parts[2]

try:
    locale.setlocale(locale.LC_TIME, "pl_PL.UTF-8")
except:
    pass

MIESIACE = [
    "STYCZEŃ", "LUTY", "MARZEC", "KWIECIEŃ", "MAJ", "CZERWIEC",
    "LIPIEC", "SIERPIEŃ", "WRZESIEŃ", "PAŹDZIERNIK", "LISTOPAD", "GRUDZIEŃ"
]

ZABLOKOWANE_MIESIACE = {"STYCZEŃ", "LUTY"}  # lub dowolnie inne

SUMMARY_COLUMNS = [
    "Oddział", "Dział", "Nazwisko I Imię",
    "CYRKI", "FOTO", "POMPY", "serwisy", "FG", "LEADY",
    "Bułgaria cyrki", "Bułgaria serwisy", "Chorwacja foto", "Chorwacja serwisy",
    "SNG cyrki", "Rumunia cyrki", "Hiszpania FOTO", "Hiszpania serwisy",
    "Chile cyrki", "Indie cyrki", "USA cyrki", "Holandia serwisy"
]

columns_admin = (
    "ID", "DATA", "OSOBA", "KRAJ", "ZADANIE", "DEKLAROWANY CZAS", "OPIS", "ILOŚĆ",
    "DEKLAROWANY x ILOŚĆ", "CZAS OD", "CZAS DO", "SUMA CZASU", "SZARA STREFA", "AKCJA"
)
columns_user = (
    "ID", "DATA", "OSOBA", "KRAJ", "ZADANIE", "DEKLAROWANY CZAS", "OPIS", "ILOŚĆ",
    "CZAS OD", "CZAS DO", "SUMA CZASU", "AKCJA"
)

def get_active_names_for_month(miesiac):
    conn = connect_db()
    cur = conn.cursor()
    miesiac_idx = MIESIACE.index(miesiac) + 1
    year = datetime.now().year
    cur.execute("""
        SELECT DISTINCT osoba
        FROM raport_ecp
        WHERE MONTH(data) = %s AND YEAR(data) = %s
    """, (miesiac_idx, year))
    osoby = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    user_names = [
        USER_TO_NAME.get(o, o)
        for o in osoby
        if o in USERS and USERS[o]["role"] == "user"
    ]
    return user_names

def generuj_procenty_dla_miesiaca(miesiac):
    aktywni = get_active_names_for_month(miesiac)
    wiersze = []
    for nazwisko in aktywni:
        wiersz = (
            "KIELCE", "Dział BOT", nazwisko,
            *["0%"] * (len(SUMMARY_COLUMNS) - 3)
        )
        wiersze.append(wiersz)
    return wiersze

# Wyznacz obecny miesiąc jako domyślny (zawsze)
obecny_idx = datetime.now().month - 1
domyslny_miesiac = MIESIACE[obecny_idx] if obecny_idx < len(MIESIACE) else MIESIACE[0]

def load_dynamic_procenty(miesiac):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT dane_json FROM procenty_miesiac WHERE miesiac = %s", (miesiac,))
    res = cur.fetchone()
    conn.close()
    if res and res[0]:
        data = json.loads(res[0])
        return data["columns"], data["rows"]
    else:
        return [], []
    
def panel_procentowy(parent):
    procent_frame = tb.Frame(parent)
    procent_frame.pack(fill=BOTH, expand=YES)
    control_panel = tb.Frame(procent_frame)
    control_panel.pack(fill=X, pady=15)
    tb.Label(control_panel, text="WYBÓR MIESIĄCA", font=("Segoe UI", 11, "bold")).pack(side=LEFT, padx=15)
    month_var = StringVar(value=domyslny_miesiac)
    month_menu = tb.Combobox(
        control_panel,
        textvariable=month_var,
        values=[m for m in MIESIACE if m not in ZABLOKOWANE_MIESIACE],
        width=14, state="readonly"
    )
    month_menu.pack(side=LEFT, padx=5)
    btn_add = tb.Button(control_panel, text="Dodaj wiersz", bootstyle="info-outline")
    btn_add.pack(side=LEFT, padx=12)
    btn_del = tb.Button(control_panel, text="Usuń wiersz", bootstyle="danger-outline")
    btn_del.pack(side=LEFT, padx=4)
    btn_import = tb.Button(control_panel, text="Importuj podział procentowy (XLSX/CSV)", bootstyle="primary-outline")
    btn_import.pack(side=RIGHT, padx=4)
    btn_export_xlsx = tb.Button(control_panel, text="Eksportuj do XLSX", bootstyle="success-outline")
    btn_export_xlsx.pack(side=RIGHT, padx=4)
    sum_table_frame = tb.Frame(procent_frame)
    sum_table_frame.pack(fill=BOTH, expand=YES, padx=20, pady=8)
    yscroll = tb.Scrollbar(sum_table_frame, orient=VERTICAL)
    yscroll.pack(side=RIGHT, fill=Y)
    xscroll = tb.Scrollbar(sum_table_frame, orient=HORIZONTAL)
    xscroll.pack(side=BOTTOM, fill=X)
    sum_table = tb.Treeview(
        sum_table_frame,
        columns=[],
        show="headings",
        bootstyle="dark",
        height=18,
        yscrollcommand=yscroll.set,
        xscrollcommand=xscroll.set
    )
    yscroll.config(command=sum_table.yview)
    xscroll.config(command=sum_table.xview)
    sum_table.pack(fill=BOTH, expand=YES)

    def edit_cell_percent(event):
        focus = sum_table.focus()
        if not focus:
            return
        x, y = event.x, event.y
        col = sum_table.identify_column(x)
        col_idx = int(col[1:]) - 1
        columns = list(sum_table["columns"])
        if col_idx < 0 or col_idx >= len(columns):
            return
        col_name = columns[col_idx]
        rowid = focus
        bbox = sum_table.bbox(rowid, col)
        if not bbox:
            return
        old_value = sum_table.set(rowid, col_name)
        entry_popup = tk.Entry(sum_table)
        entry_popup.place(x=bbox[0], y=bbox[1], width=bbox[2], height=bbox[3])
        entry_popup.insert(0, old_value)
        entry_popup.focus_set()
        def save_edit(event=None):
            new_value = entry_popup.get()
            sum_table.set(rowid, col_name, new_value)
            entry_popup.destroy()
            update_month_data()
        entry_popup.bind("<Return>", save_edit)
        entry_popup.bind("<FocusOut>", save_edit)

    sum_table.bind("<Double-1>", edit_cell_percent)

    def fit_columns_to_content(tree):
        # Pobierz font z nagłówka (to najbardziej zbliżone do tego co widzisz)
        style = ttk.Style()
        font_name = style.lookup("Treeview.Heading", "font")
        if not font_name:
            font_name = "TkDefaultFont"
        font = tkfont.nametofont(font_name)
        for col in tree["columns"]:
            header_width = font.measure(col) + 28
            max_cell_width = header_width
            for item in tree.get_children():
                value = tree.set(item, col)
                cell_width = font.measure(str(value)) + 16
                if cell_width > max_cell_width:
                    max_cell_width = cell_width
            tree.column(col, width=max_cell_width, minwidth=60)

    def refresh_summary_table(*_):
        sum_table.delete(*sum_table.get_children())
        month = month_var.get().upper()
        # OD LIPCA 2024 liczymy z raportu!
        idx = MIESIACE.index(month)
        if idx >= 6:  # 6 = lipiec (liczone od 0)
            columns, rows = calculate_procenty_for_month(month)
        else:
            columns, rows = load_dynamic_procenty(month)
        if not columns:
            return
        sum_table["columns"] = columns
        for idx, col in enumerate(columns):
            width = 120 if "Nazwisko" not in col else 180
            sum_table.heading(col, text=col)
            sum_table.column(col, width=width, anchor="center", stretch=(idx == len(columns) - 1))
        for row in rows:
            sum_table.insert('', 'end', values=row)
        fit_columns_to_content(sum_table)
    month_menu.bind("<<ComboboxSelected>>", lambda e: refresh_summary_table())
    
    def fetch_naglowki():
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("SELECT naglowki FROM naglowki LIMIT 1")  # lub WHERE, jeśli masz wiele zestawów
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            # fallback
            return ["Oddział", "Dział", "Nazwisko i Imię"]
        return json.loads(row[0])

    def calculate_procenty_for_month(miesiac):
        miesiac_idx = MIESIACE.index(miesiac) + 1
        year = datetime.now().year

        # Pobierz nagłówki (kolumny)
        columns = fetch_naglowki()
        rodzaje = columns[3:]

        conn = connect_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT r.osoba, z.rodzaj,z.kraj, r.czas_od, r.czas_do
            FROM raport_ecp r
            LEFT JOIN zadania z ON r.zadanie = z.zadanie AND r.kraj = z.kraj
            WHERE MONTH(r.data) = %s AND YEAR(r.data) = %s
            ORDER BY r.osoba, r.czas_od
        """, (miesiac_idx, year))
        records = cur.fetchall()
        conn.close()
        users_tasks = {}
        for osoba, rodzaj, kraj, czas_od, czas_do in records:
            rodzaj = rodzaj or "INNE"
            if not osoba or not rodzaj or not czas_od or not czas_do:
                continue
            try:
                t1 = datetime.strptime(str(czas_od), "%H:%M:%S")
                t2 = datetime.strptime(str(czas_do), "%H:%M:%S")
                delta = (t2 - t1).total_seconds()
                if delta < 0: delta += 24 * 3600  # przekroczenie północy
            except Exception:
                continue
            users_tasks.setdefault(osoba, {})
            users_tasks[osoba][rodzaj] = users_tasks[osoba].get(rodzaj, 0) + delta

        rows = []
        for osoba, rodzaje_dict in users_tasks.items():
            # POPRAWKA: sumuj tylko summary columns!
            sum_all = sum(rodzaje_dict.get(rodzaj, 0) for rodzaj in rodzaje)
            if not sum_all:
                continue
            row = ["KIELCE", "Dział BOT", USER_TO_NAME.get(osoba, osoba)]
            for rodzaj in rodzaje:
                czas = rodzaje_dict.get(rodzaj, 0)
                percent = (czas / sum_all) * 100 if sum_all else 0
                row.append(f"{percent:.0f}%" if czas else "0%")
            rows.append(row)
        # *** NA SAMYM KOŃCU ***
        return columns, rows

    def import_procentowy_xlsx_csv():
        miesiac = month_var.get().upper()
        file_path = filedialog.askopenfilename(
            title="Wybierz plik XLSX lub CSV",
            filetypes=[("Excel files", "*.xlsx"), ("CSV files", "*.csv")]
        )
        if not file_path:
            return

        # Pobierz aktualny podział
        columns, rows = load_dynamic_procenty(miesiac)
        if not columns:
            columns = fetch_naglowki()
            rows = []

        istniejące_osoby = set(r[2].strip().lower() for r in rows if len(r) > 2)

        # Wczytaj plik do df
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        elif file_path.endswith('.csv'):
            try:
                df = pd.read_csv(file_path, delimiter=";")
            except Exception:
                df = pd.read_csv(file_path, delimiter=",")
        else:
            messagebox.showerror("Błąd", "Nieobsługiwany format pliku (musi być .xlsx lub .csv)")
            return

        # Ustal indeks kolumny z nazwiskiem
        nazwy_kolumn = [c.lower() for c in df.columns]
        col_nazwisko = None
        for name in ["nazwisko i imię", "nazwisko imię", "pracownik"]:
            if name in nazwy_kolumn:
                col_nazwisko = nazwy_kolumn.index(name)
                break
        if col_nazwisko is None:
            messagebox.showerror("Błąd", "W pliku musi być kolumna 'Nazwisko i Imię' lub 'Nazwisko Imię' lub 'Pracownik'")
            return

        nowe_wiersze = []
        ile_pominieto = 0
        for i, row_import in df.iterrows():
            nazwisko_imie = str(row_import.iloc[col_nazwisko]).strip()
            if not nazwisko_imie:
                continue
            if nazwisko_imie.lower() in istniejące_osoby:
                ile_pominieto += 1
                continue  # już jest — pomijamy!
            # Spróbuj pobrać wartości podziału jeśli układ kolumn się zgadza
            nowy = []
            for idx, col in enumerate(columns):
                col_low = col.lower()
                if idx == 0:
                    nowy.append("KIELCE")
                elif idx == 1:
                    nowy.append("Dział BOT")
                elif idx == 2:
                    nowy.append(nazwisko_imie)
                else:
                    # jeśli taka kolumna jest w pliku — bierz z pliku, jeśli nie: "0%"
                    if col_low in nazwy_kolumn:
                        try:
                            nowy.append(str(row_import.iloc[nazwy_kolumn.index(col_low)]))
                        except Exception:
                            nowy.append("0%")
                    else:
                        nowy.append("0%")
            nowe_wiersze.append(nowy)

        if not nowe_wiersze:
            messagebox.showinfo("Import", f"Nie znaleziono nowych osób do dodania.\n{ile_pominieto} osób z pliku już istnieje.")
            return

        rows.extend(nowe_wiersze)
        save_dynamic_procenty(miesiac, columns, rows)
        refresh_summary_table()
        messagebox.showinfo(
            "Import zakończony",
            f"Dodano {len(nowe_wiersze)} nowych osób do tabeli na {miesiac.capitalize()}.\nPominięto {ile_pominieto} osób, które już były w tabeli."
        )
    btn_import.config(command=import_procentowy_xlsx_csv)

    def export_to_xlsx():
        rows = []
        for iid in sum_table.get_children():
            rows.append(sum_table.item(iid)['values'])
        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
            title="Zapisz tabelę jako XLSX"
        )
        if not file_path:
            return
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Podział procentowy"
        ws.append(list(sum_table["columns"]))
        for cell in ws[1]:
            cell.font = Font(bold=True)
        for row in rows:
            ws.append(list(row))
        for column_cells in ws.columns:
            length = max(len(str(cell.value)) for cell in column_cells)
            ws.column_dimensions[column_cells[0].column_letter].width = length + 2
        wb.save(file_path)
        messagebox.showinfo("Eksport XLSX", "Plik został zapisany.")
    btn_export_xlsx.config(command=export_to_xlsx)

    def add_row():
        columns = list(sum_table["columns"])
        existing_names = fetch_nazwiska()
        popup = tk.Toplevel()
        popup.title("Dodaj nowy wiersz")
        popup.geometry("350x120")
        popup.transient(parent)
        popup.grab_set()
        center_popup(popup, parent)
        tk.Label(popup, text="Wybierz lub wpisz Nazwisko i Imię:", font=("Segoe UI", 11)).pack(pady=(14, 5))
        name_var = tk.StringVar()
        name_box = ttk.Combobox(popup, textvariable=name_var, values=existing_names, width=28)
        name_box.pack(pady=(0, 10))
        name_box.focus_set()
        def confirm():
            nazwisko_imie = name_var.get().strip()
            if not nazwisko_imie:
                messagebox.showerror("Błąd", "Musisz wpisać Nazwisko i Imię")
                return
            add_nazwisko_db(nazwisko_imie)
            oddzial = "KIELCE"
            dzial = "Dział BOT"
            new_row = [oddzial, dzial, nazwisko_imie] + ["0%"] * (len(columns) - 3)
            sum_table.insert('', 'end', values=new_row)
            update_month_data()
            popup.destroy()
        tk.Button(popup, text="Dodaj", command=confirm, width=16).pack(pady=(2, 12))
        popup.bind("<Return>", lambda event: confirm())

    btn_add.config(command=add_row)

    def delete_selected_row():
        selected = sum_table.selection()
        if not selected:
            messagebox.showwarning("Brak wyboru", "Zaznacz wiersz do usunięcia.")
            return
        if messagebox.askyesno("Potwierdź usunięcie", "Czy na pewno chcesz usunąć zaznaczony wiersz?"):
            for item in selected:
                sum_table.delete(item)
            update_month_data()
    btn_del.config(command=delete_selected_row)

    def update_month_data():
        month = month_var.get().upper()
        columns = list(sum_table["columns"])
        data = []
        for iid in sum_table.get_children():
            row = [sum_table.item(iid)['values'][i] if i < len(sum_table.item(iid)['values']) else "" for i in range(len(columns))]
            data.append(row)
        save_dynamic_procenty(month, columns, data)

    refresh_summary_table()
    return procent_frame

def panel_raport_ecp(parent, username, is_admin=False):
    raport_frame = tb.Frame(parent)
    raport_frame.pack(fill=BOTH, expand=YES)
    is_dt = username.startswith("dzial_techniczny")

    # --- PANEL GÓRNY ---
    top_frame = tb.Frame(raport_frame)
    top_frame.pack(pady=12, padx=8, fill=X)
    tb.Label(top_frame, text="").pack(side=LEFT, expand=YES)
    center_panel = tb.Frame(top_frame)
    center_panel.pack(side=LEFT, expand=YES)
    label_cfg = {'padx': 4, 'pady': 1, 'sticky': 'ew'}
    entry_cfg = {'padx': 4, 'pady': 2, 'sticky': 'ew'}

    tb.Label(center_panel, text="DATA", width=11, anchor="center").grid(row=0, column=0, **label_cfg)
    entry_data = tb.Entry(center_panel, width=12)
    entry_data.insert(0, datetime.now().strftime("%d.%m.%Y"))
    entry_data.grid(row=1, column=0, **entry_cfg)

    # --- START PRACY ---
    tb.Label(center_panel, text="START PRACY", width=15, anchor="center").grid(row=0, column=1, padx=4, pady=1, sticky='ew')
    start_var = tk.StringVar(value="")
    entry_start = tb.Entry(center_panel, textvariable=start_var, width=9, justify="center", bootstyle="dark")
    entry_start.grid(row=1, column=1, padx=4, pady=2, sticky='ew')
    btn_start = tb.Button(center_panel, text="START", bootstyle=SUCCESS, width=9)
    btn_start.grid(row=1, column=2, padx=4, pady=2, sticky='ew')

    def czy_masz_otwarta_zmiane(username, data):
        """
        Zwraca True jeśli użytkownik ma rozpoczętą zmianę na dany dzień, ale nie zamkniętą (czyli czas_od jest, a czas_do brak).
        """
        zmiana = get_zmiana_na_dzien(username, data)
        # zmiana: (id, czas_od, czas_do)
        return zmiana and zmiana[1] and not zmiana[2]
    
    def on_edit_start_time(event=None):
        user = osoba_var.get()
        data = entry_data.get()
        now_start = start_var.get()
        try:
            data_mysql = datetime.strptime(data, "%d.%m.%Y").strftime("%Y-%m-%d")
            conn = connect_db()
            cur = conn.cursor()
            if now_start:
                cur.execute(
                    "UPDATE raport_zmiany SET czas_od=%s WHERE osoba=%s AND data=%s",
                    (now_start, user, data_mysql)
                )
            conn.commit()
            conn.close()
        except Exception as e:
            messagebox.showerror("Błąd", f"Nie udało się zapisać czasu rozpoczęcia: {e}")

    entry_start.bind("<FocusOut>", on_edit_start_time)
    entry_start.bind("<Return>", on_edit_start_time)

    # --- KONIEC PRACY ---
    tb.Label(center_panel, text="KONIEC PRACY", width=15, anchor="center").grid(row=0, column=3, padx=4, pady=1, sticky='ew')
    koniec_var = tk.StringVar(value="")
    entry_koniec = tb.Entry(center_panel, textvariable=koniec_var, width=9, justify="center", bootstyle="dark")
    entry_koniec.grid(row=1, column=3, padx=4, pady=2, sticky='ew')
    btn_koniec = tb.Button(center_panel, text="KONIEC", bootstyle=DANGER, width=9)
    btn_koniec.grid(row=1, column=4, padx=4, pady=2, sticky='ew')

    def on_edit_end_time(event=None):
        user = osoba_var.get()
        data = entry_data.get()
        now_end = koniec_var.get()
        try:
            data_mysql = datetime.strptime(data, "%d.%m.%Y").strftime("%Y-%m-%d")
            conn = connect_db()
            cur = conn.cursor()
            if now_end:
                cur.execute(
                    "UPDATE raport_zmiany SET czas_do=%s WHERE osoba=%s AND data=%s",
                    (now_end, user, data_mysql)
                )
            conn.commit()
            conn.close()
        except Exception as e:
            messagebox.showerror("Błąd", f"Nie udało się zapisać czasu zakończenia: {e}")

    entry_koniec.bind("<FocusOut>", on_edit_end_time)
    entry_koniec.bind("<Return>", on_edit_end_time)

    tb.Label(center_panel, text="OSOBA", width=13, anchor="center").grid(row=0, column=5, **label_cfg)
    osoba_var = tk.StringVar(value=username)
    osoba_entry = tb.Entry(center_panel, textvariable=osoba_var, width=20, state="readonly", justify="center")
    osoba_entry.grid(row=1, column=5, **entry_cfg)

    tb.Label(center_panel, text="KRAJ", width=10, anchor="center").grid(row=0, column=6, **label_cfg)
    kraj_var = tk.StringVar(value="")
    kraje_list = fetch_kraje()
    kraj_menu = tb.Combobox(center_panel, textvariable=kraj_var, values=kraje_list, width=15, state="readonly")
    kraj_menu.grid(row=1, column=6, **entry_cfg)
    kraj_menu.bind("<<ComboboxSelected>>", lambda e: refresh_zadania())
    tb.Button(center_panel, text="+", width=2, command=lambda: add_kraj()).grid(row=1, column=7, padx=(0, 8))

    tb.Label(center_panel, text="ZADANIE", width=20, anchor="center").grid(row=0, column=8, **label_cfg)
    zadanie_var = tk.StringVar(value="")
    zadania_list = fetch_zadania(kraj_var.get())
    zadanie_menu = tb.Combobox(center_panel, textvariable=zadanie_var, values=zadania_list, width=44, state="readonly")
    zadanie_menu.grid(row=1, column=8, **entry_cfg)

    tb.Label(center_panel, text="OPIS DODATKOWY", width=20, anchor="center").grid(row=0, column=10, **label_cfg)
    entry_opis = tb.Entry(center_panel, width=26)
    entry_opis.grid(row=1, column=10, **entry_cfg)

    tb.Label(center_panel, text="ILOŚĆ PREZENTACJI", width=20, anchor="center").grid(row=0, column=11, **label_cfg)
    entry_ilosc = tb.Entry(center_panel, width=10)
    entry_ilosc.grid(row=1, column=11, **entry_cfg)
    for i in range(12):
        center_panel.grid_columnconfigure(i, weight=1)
    tb.Label(top_frame, text="").pack(side=LEFT, expand=YES)

    real_start, real_end = get_real_start_end_time(username, entry_data.get())
    start_var.set(real_start or "")
    koniec_var.set(real_end or "")

    def rozpocznij_prace():
        open_shift = get_zmiana_na_dzien(username, entry_data.get())
        if open_shift and open_shift[1] and not open_shift[2]:
            messagebox.showinfo("Uwaga", "Zmiana już rozpoczęta!")
            return
        # DODAJ: zamykanie innych niedomkniętych zmian z poprzednich dni
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("""
            UPDATE raport_zmiany SET czas_do=%s WHERE osoba=%s AND czas_do IS NULL
        """, (datetime.now().strftime("%H:%M:%S"), username))
        conn.commit()
        conn.close()
        now = rozpocznij_zmiane(username, entry_data.get())
        start_var.set(now)
        koniec_var.set("")
        btn_dodaj.config(state=tk.NORMAL)

    def refresh_zadania():
        zadania_list = fetch_zadania(kraj_var.get())
        zadanie_menu.config(values=zadania_list)
        zadanie_var.set("")

    def zakoncz_prace():
        user = osoba_var.get()
        data = entry_data.get()
        try:
            data_mysql = datetime.strptime(data, "%d.%m.%Y").strftime("%Y-%m-%d")
        except Exception:
            data_mysql = datetime.now().strftime("%Y-%m-%d")
        now = datetime.now().strftime("%H:%M:%S")
        conn = connect_db()
        cur = conn.cursor()
        # Zamykamy wszystkie niedomknięte zmiany DLA TEGO DNIA
        cur.execute("""
            UPDATE raport_zmiany 
            SET czas_do=%s 
            WHERE osoba=%s 
            AND czas_do IS NULL
        """, (now, user))
        # Zamykamy wszystkie otwarte zadania ECP tego dnia
        cur.execute("""
            UPDATE raport_ecp
            SET czas_do=%s
            WHERE osoba=%s
            AND data=%s
            AND czas_do IS NULL
        """, (now, user, data_mysql))
        conn.commit()
        conn.close()
        koniec_var.set(now)
        btn_dodaj.config(state=tk.DISABLED)
        messagebox.showinfo("Zamknięcie zmiany", "Zmiana oraz wszystkie rozpoczęte zadania zostały zakończone.")
        start, end = get_real_start_end_time(username, entry_data.get())
        start_var.set(start or "")
        koniec_var.set(end or "")
    btn_start.config(command=rozpocznij_prace)
    btn_koniec.config(command=zakoncz_prace)

    def refresh_kraje_and_zadania():
        kraje_list_new = fetch_kraje()
        kraj_menu.config(values=kraje_list_new)
        if kraj_var.get() not in kraje_list_new:
            kraj_var.set("")
        zadania_list_new = fetch_zadania(kraj_var.get())
        zadanie_menu.config(values=zadania_list_new)
        zadanie_var.set("")

    def add_kraj():
        new_kraj = simpledialog.askstring("Dodaj kraj", "Podaj skrót kraju (np. PL, HR, DE):")
        if new_kraj:
            add_kraj_db(new_kraj.upper())
            refresh_kraje_and_zadania()
            kraj_var.set(new_kraj.upper())

    def add_zadanie():
        kraj = kraj_var.get()
        if not kraj:
            messagebox.showerror("Błąd", "Najpierw wybierz kraj!")
            return

        popup = tk.Toplevel()
        popup.title("Dodaj zadanie")
        popup.geometry("360x170")
        popup.transient(center_panel)
        popup.grab_set()
        center_popup(popup, center_panel)

        tk.Label(popup, text="Nazwa zadania:", font=("Segoe UI", 11)).pack(pady=(18, 2))
        zad_var = tk.StringVar()
        entry_zad = tk.Entry(popup, textvariable=zad_var, font=("Segoe UI", 10), width=35)
        entry_zad.pack(pady=(0, 6))

        tk.Label(popup, text="Deklarowany czas (hh:mm:ss) – opcjonalnie:", font=("Segoe UI", 10)).pack(pady=(4, 2))
        czas_var = tk.StringVar()
        entry_czas = tk.Entry(popup, textvariable=czas_var, font=("Segoe UI", 10), width=18)
        entry_czas.pack(pady=(0, 12))

        entry_zad.focus_set()

        def confirm():
            new_zad = zad_var.get().strip()
            czas = czas_var.get().strip()
            if not new_zad:
                messagebox.showerror("Błąd", "Wpisz nazwę zadania.")
                return
            conn = connect_db()
            cur = conn.cursor()
            cur.execute(
                "INSERT IGNORE INTO zadania (kraj, zadanie, czas_deklarowany) VALUES (%s, %s, %s)",
                (kraj, new_zad, czas if czas else None)
            )
            conn.commit()
            conn.close()
            zadania_new = fetch_zadania(kraj)
            zadanie_menu.config(values=zadania_new)
            zadanie_var.set(new_zad)
            popup.destroy()

        btn = tk.Button(popup, text="Dodaj zadanie", font=("Segoe UI", 11), width=16, command=confirm)
        btn.pack(pady=(4, 14))
        popup.bind("<Return>", lambda e: confirm())

    # --- KOLUMNY I TABLE ---
    if is_admin:
        columns = (
            "ID", "DATA", "OSOBA", "KRAJ", "ZADANIE", "DEKLAROWANY CZAS", "OPIS", "ILOŚĆ",
            "DEKLAROWANY x ILOŚĆ", "CZAS OD", "CZAS DO", "SUMA CZASU", "SZARA STREFA", "AKCJA", "USUŃ", "ID_BAZA"
        )
        
    elif is_dt:
        columns = (
            "ID", "DATA", "OSOBA", "KRAJ", "ZADANIE", "DEKLAROWANY CZAS", "OPIS", "ILOŚĆ",
            "DEKLAROWANY x ILOŚĆ", "CZAS OD", "CZAS DO", "SUMA CZASU", "SZARA STREFA", "AKCJA", "ID_BAZA"
        )

    widths = {
        "ID": 40, "DATA": 65, "OSOBA": 110, "KRAJ": 80, "ZADANIE": 260, "DEKLAROWANY CZAS": 100, "OPIS": 160,
        "ILOŚĆ": 45, "DEKLAROWANY x ILOŚĆ": 150, "CZAS OD": 65, "CZAS DO": 65, "SUMA CZASU": 80,
        "SZARA STREFA": 95, "AKCJA": 85, "USUŃ": 45
    }

    table_frame = tb.Frame(raport_frame)
    table_frame.pack(fill=BOTH, expand=YES, padx=18, pady=2)
    yscroll = tb.Scrollbar(table_frame, orient=VERTICAL)
    yscroll.pack(side=RIGHT, fill=Y)
    xscroll = tb.Scrollbar(table_frame, orient=HORIZONTAL)
    xscroll.pack(side=BOTTOM, fill=X)
    table = tb.Treeview(
        table_frame,
        columns=columns,
        show="headings",
        height=32,
        bootstyle="dark",
        yscrollcommand=yscroll.set,
        xscrollcommand=xscroll.set
    )
    table.tag_configure('w_trakcie', foreground='yellow')
    yscroll.config(command=table.yview)
    xscroll.config(command=table.xview)
    last_visible_idx = len(columns) - 2 if columns[-1] == "ID_BAZA" else len(columns) - 1

    for idx, col in enumerate(columns):
        if col == "ID_BAZA":
            table.column(col, width=0, minwidth=0, stretch=False)
            continue
        table.heading(col, text=col, anchor="center")
        stretch_col = True if idx == last_visible_idx else False
        table.column(col, width=widths.get(col, 110), anchor="center", stretch=stretch_col)
    table.pack(fill="both", expand=True)

    def refresh_table():
        table.delete(*table.get_children())
        records = fetch_all_records(
            user=username, 
            parent=table.winfo_toplevel(), 
            refresh_callback=refresh_table
        )
        # Sortujemy po dacie rosnąco (najstarszy najpierw)
        sorted_records = sorted(
            [r for r in records if r[1] is not None],
            key=lambda r: (
                r[1] if isinstance(r[1], (datetime, date)) and r[1] is not None
                else datetime.strptime(r[1], "%Y-%m-%d") if r[1]
                else datetime(1900, 1, 1),
                datetime.strptime(str(r[7]), "%H:%M:%S") if r[7] else datetime.min
            ),
            reverse=False
        )

        narastajace_szare = []
        ostatni_dzien = None
        szara_td = timedelta()
        ostatni_koniec = None

        for rec in sorted_records:
            zadanie = rec[4]
            czas_od = str(rec[7]) if rec[7] else ""
            czas_do = str(rec[8]) if rec[8] else ""
            data = rec[1]
            dzien = data.strftime("%d.%m.%Y") if hasattr(data, "strftime") else str(data)
            
            # Nowy dzień = reset liczników!
            if ostatni_dzien != dzien:
                szara_td = timedelta()
                ostatni_koniec = None
                ostatni_dzien = dzien

            # Liczymy SZARĄ STREFĘ: tylko luki między zadaniami
            if ostatni_koniec and czas_od:
                try:
                    t_do = datetime.strptime(ostatni_koniec, "%H:%M:%S")
                    t_od = datetime.strptime(czas_od, "%H:%M:%S")
                    diff = t_od - t_do
                    # Liczymy tylko odstępy do max 4 godzin (filtrowanie anomalii!)
                    if 0 < diff.total_seconds() < 4*3600:
                        szara_td += diff
                except Exception:
                    pass

            narastajace_szare.append(szara_td)
            # Zapisz ostatni czas_do (tylko jeśli nie jest pusty)
            if czas_do:
                ostatni_koniec = czas_do

        # Teraz nadajemy ID od 1 do n (najstarszy 1, najnowszy n)
        total = len(sorted_records)

        for idx, (rec, szara_na_ten_wiersz) in enumerate(zip(reversed(sorted_records), reversed(narastajace_szare)), 1):
            lokalny_id = total - idx + 1  # najnowszy ma najwyższy numer

            (
                id_z_bazy, data, osoba, kraj, zadanie, opis, ilosc, czas_od, czas_do
            ) = rec[:9]
            date_str = data.strftime("%d.%m.%Y") if hasattr(data, "strftime") else str(data)
            deklarowany_czas = CZASY_DEKLAROWANE.get(zadanie, {}).get(kraj, "")
            if deklarowany_czas:
                sekundy = hms_to_seconds(deklarowany_czas)
                deklarowany_x_ilosc = seconds_to_hms(sekundy * int(ilosc or 0))
            else:
                deklarowany_x_ilosc = ""
            suma_czasu = time_diff(czas_od, czas_do)

            row = [
                lokalny_id,
                date_str,
                osoba,
                kraj,
                zadanie,
                deklarowany_czas,
                opis,
                ilosc,
                deklarowany_x_ilosc,
                str(czas_od) if czas_od else "",
                str(czas_do) if czas_do else "",
                suma_czasu,
                seconds_to_hms(szara_na_ten_wiersz.total_seconds()),
                "Zakończono" if czas_do else "STOP"
            ]
            if is_admin:
                row.append("❌")
            row.append(id_z_bazy)

            while len(row) < len(columns):
                row.append("")
            if len(row) > len(columns):
                row = row[:len(columns)]

            row = [x if x not in (None, "None") else "" for x in row]

            idx_akcja = list(columns).index("AKCJA")
            if row[idx_akcja] == "STOP":
                table.insert('', 'end', values=row, tags=('w_trakcie',))
            else:
                table.insert('', 'end', values=row)

        # --- TU KONTROLA BLOKADY DODAJ ---
        w_trakcie = any(not r[8] for r in records)
        data_str = entry_data.get()
        ma_zmiane = czy_masz_otwarta_zmiane(username, data_str)
        if not ma_zmiane:
            btn_dodaj.config(state=tk.DISABLED)
        else:
            btn_dodaj.config(state=tk.DISABLED if w_trakcie else tk.NORMAL)
            
    def on_treeview_scroll(*args):
        table.yview(*args)
        if table.yview()[1] > 0.97:
            refresh_table()
    yscroll.config(command=on_treeview_scroll)

    def edit_cell_admin(event):
        focus = table.focus()
        if not focus:
            return
        x, y = event.x, event.y
        col = table.identify_column(x)
        col_idx = int(col[1:]) - 1
        columns_list = list(columns)  # użyj dokładnie tej, którą masz przy tworzeniu tabeli
        col_name = columns_list[col_idx]
        # Uprawnienia: user może edytować tylko OPIS i ILOŚĆ, admin wszystko oprócz ID i USUŃ
        # Nowe
        if col_name in ("ID", "USUŃ"):
            return
        rowid = focus
        bbox = table.bbox(rowid, col)
        if not bbox:
            return
        old_value = table.set(rowid, col_name)
        entry_popup = tk.Entry(table)
        entry_popup.place(x=bbox[0], y=bbox[1], width=bbox[2], height=bbox[3])
        entry_popup.insert(0, old_value)
        entry_popup.focus_set()

        def save_edit(event=None):
            new_value = entry_popup.get()
            if new_value == old_value:
                entry_popup.destroy()
                return
            # Walidacja ilości
            if col_name == "ILOŚĆ":
                try:
                    new_value = int(new_value)
                except Exception:
                    messagebox.showerror("Błąd", "Ilość musi być liczbą całkowitą.")
                    entry_popup.focus_set()
                    return
            # Walidacja daty
            if col_name == "DATA":
                try:
                    datetime.strptime(new_value, "%d.%m.%Y")
                except Exception:
                    messagebox.showerror("Błąd", "Data musi być w formacie DD.MM.YYYY.")
                    entry_popup.focus_set()
                    return
            # Walidacja czasu
            if col_name in ("CZAS OD", "CZAS DO"):
                try:
                    datetime.strptime(new_value, "%H:%M:%S")
                except Exception:
                    messagebox.showerror("Błąd", "Czas musi być w formacie HH:MM:SS.")
                    entry_popup.focus_set()
                    return

            values = list(table.item(rowid, "values"))
            # Pobierz ID_BAZA (ostatnia kolumna)
            id_baza_idx = list(columns).index("ID_BAZA")
            id_baza = values[id_baza_idx]

            mapping = {
                "DATA": "data",
                "OSOBA": "osoba",
                "KRAJ": "kraj",
                "ZADANIE": "zadanie",
                "OPIS": "opis",
                "ILOŚĆ": "ilosc",
                "CZAS OD": "czas_od",
                "CZAS DO": "czas_do",
            }
            sql_col = mapping.get(col_name)

            # Zapis do MySQL:
            if sql_col:
                conn = connect_db()
                cur = conn.cursor()
                val = new_value
                if col_name == "DATA":
                    val = datetime.strptime(new_value, "%d.%m.%Y").strftime("%Y-%m-%d")
                cur.execute(f"UPDATE raport_ecp SET {sql_col}=%s WHERE id=%s", (val, id_baza))
                conn.commit()
                conn.close()

            table.set(rowid, col_name, new_value)
            entry_popup.destroy()

            # Przelicz „DEKLAROWANY x ILOŚĆ” jeśli zmieniono ILOŚĆ
            if col_name == "ILOŚĆ":
                zadanie = values[columns_list.index("ZADANIE")]
                kraj = values[columns_list.index("KRAJ")] 
                ilosc = int(new_value)
                deklarowany_czas = CZASY_DEKLAROWANE.get(zadanie, {}).get(kraj, "")
                if deklarowany_czas:
                    sekundy = hms_to_seconds(deklarowany_czas)
                    deklarowany_x_ilosc = seconds_to_hms(sekundy * ilosc)
                else:
                    deklarowany_x_ilosc = ""
                idx_deklarowany = columns_list.index("DEKLAROWANY x ILOŚĆ")
                values[idx_deklarowany] = deklarowany_x_ilosc
                table.item(rowid, values=values)
            refresh_table()

        entry_popup.bind("<Return>", save_edit)
        entry_popup.bind("<FocusOut>", save_edit)


    def delete_row(event):
        if not is_admin:
            return
        region = table.identify("region", event.x, event.y)
        col = table.identify_column(event.x)
        col_idx = int(col[1:]) - 1
        row_id = table.identify_row(event.y)
        if col_idx == list(columns).index("USUŃ") and row_id:
            values = table.item(row_id, "values")
            id_ = values[0]
            if messagebox.askyesno("Potwierdź usunięcie", "Usunąć ten wpis?"):
                conn = connect_db()
                cur = conn.cursor()
                cur.execute("DELETE FROM raport_ecp WHERE id=%s", (id_,))
                conn.commit()
                conn.close()
                table.delete(row_id)

    def dodaj_rekord():
        czas_od = datetime.now().strftime("%H:%M:%S")
        try:
            data_mysql = datetime.strptime(entry_data.get(), "%d.%m.%Y").strftime("%Y-%m-%d")
        except Exception:
            messagebox.showerror("Błąd", "Data musi być w formacie DD.MM.YYYY")
            return
        row = (
            data_mysql,
            osoba_var.get(),
            kraj_var.get(),
            zadanie_var.get(),
            entry_opis.get(),
            int(entry_ilosc.get() or 0) if entry_ilosc else 0,
            czas_od,
            None
        )
        insert_record(row)
        refresh_table()
        kraj_var.set("")
        zadanie_var.set("")
        entry_opis.delete(0, 'end')
        if entry_ilosc:
            entry_ilosc.delete(0, 'end')
        refresh_kraje_and_zadania()

    btn_dodaj = tb.Button(raport_frame, text="DODAJ", bootstyle=SUCCESS, command=dodaj_rekord, state=tk.NORMAL)
    btn_dodaj.pack(pady=18)
    

    def stop_rekord(event):
        region = table.identify("region", event.x, event.y)
        col = table.identify_column(event.x)
        row_id = table.identify_row(event.y)
        idx_akcja = list(columns).index("AKCJA")
        idx_id_baza = list(columns).index("ID_BAZA")
        if col == f"#{idx_akcja+1}" and row_id:
            values = table.item(row_id, "values")
            id_z_bazy = values[idx_id_baza]
            if values[idx_akcja].strip().upper() == "STOP":
                now = datetime.now().strftime("%H:%M:%S")
                update_czas_do(id_z_bazy, now)
                refresh_table()
            else:
                # Kliknięcie w Zakończono, nie robimy nic
                pass
        # Obsługa usuwania w adminie (jeśli jest kolumna USUŃ)
        if is_admin and "USUŃ" in columns:
            idx_usun = list(columns).index("USUŃ")
            if col == f"#{idx_usun+1}" and row_id:
                delete_row(event)

    def auto_refresh():
        refresh_table()
        raport_frame.after(30000, auto_refresh)  # co 30 sek.

    auto_refresh()
    table.bind("<Double-1>", edit_cell_admin)
    table.bind("<Button-1>", stop_rekord)
    refresh_table()
    return raport_frame

def panel_informacje_zbiorcze(parent):
    frame = tb.Frame(parent)
    frame.pack(fill=BOTH, expand=YES)

    # --- NAGŁÓWEK MIESIĘCZNY
    naglowek_miesieczne = tb.Label(
        frame, text="Miesięczna ewidencja czasu pracy",
        font=("Segoe UI", 13, "bold")
    )
    naglowek_miesieczne.pack(anchor="w", padx=12, pady=(18, 3))

    # --- PANEL WYBORU MIESIĄCA (teraz POD nagłówkiem)
    control_month = tb.Frame(frame)
    control_month.pack(anchor="w", padx=12, pady=(0, 10))
    tb.Label(control_month, text="Wybierz miesiąc:", font=("Segoe UI", 11, "bold")).pack(side=LEFT, padx=(0, 8))
    miesiac_var = StringVar(value=datetime.now().strftime("%m.%Y"))
    miesiace = [f"{i:02d}.{datetime.now().year}" for i in range(1, 13)]
    miesiac_menu = tb.Combobox(control_month, textvariable=miesiac_var, values=miesiace, width=10, state="readonly")
    miesiac_menu.pack(side=LEFT)


    miesieczne_columns = ["Oddział", "Dział", "Pracownik", "Godzin"]
    miesieczna = tb.Treeview(frame, columns=miesieczne_columns, show="headings", bootstyle="dark", height=6)
    miesieczna.pack(anchor="w", padx=12, pady=(0, 12))

    for col in miesieczne_columns:
        if col == "Pracownik":
            miesieczna.heading(col, text=col)
            miesieczna.column(col, width=200, anchor="center")
        elif col == "Godzin":
            miesieczna.heading(col, text=col)
            miesieczna.column(col, width=130, anchor="center")
        else:
            miesieczna.heading(col, text=col)
            miesieczna.column(col, width=120, anchor="center")

    # --- NAGŁÓWEK DZIENNY
    naglowek_dzienne = tb.Label(
        frame, text="Raport dzienny ECP",
        font=("Segoe UI", 13, "bold")
    )
    naglowek_dzienne.pack(anchor="w", padx=12, pady=(4, 2))

    # --- PANEL WYBORU DNIA
    control_day = tb.Frame(frame)
    control_day.pack(anchor="w", padx=12, pady=(0, 8))
    tb.Label(control_day, text="Wybierz dzień:", font=("Segoe UI", 11, "bold")).pack(side=LEFT, padx=(0, 8))
    dni_var = StringVar()
    dni_combo = tb.Combobox(control_day, textvariable=dni_var, width=12, state="readonly")
    dni_combo.pack(side=LEFT)

    dzienne_columns = [
        "Data", "Nazwisko Imię", "Czas rozpoczęcia", "Czas zakończenia", "Czas zadań",
        "Łączny czas pracy", "Przerwa prywatna", "ECP %", "Czas (Szara strefa)", "Kraj", "Bieżące zadanie", "Opis"
    ]
    dzienna = tb.Treeview(frame, columns=dzienne_columns, show="headings", bootstyle="dark", height=10)

    for col in dzienne_columns:
        if col == "Opis":
            dzienna.heading(col, text=col)
            dzienna.column(col, width=200, anchor="center")
        elif col == "Bieżące zadanie":
            dzienna.heading(col, text=col)
            dzienna.column(col, width=200, anchor="center")
        elif col == "Nazwisko Imię":
            dzienna.heading(col, text=col)
            dzienna.column(col, width=125, anchor="center")
        else:
            dzienna.heading(col, text=col)
            dzienna.column(col, width=107, anchor="center")
    scroll_x = tb.Scrollbar(frame, orient='horizontal')
    scroll_x.pack(side='bottom', fill='x')
    dzienna.config(xscrollcommand=scroll_x.set)
    scroll_x.config(command=dzienna.xview)
    dzienna.pack(fill=BOTH, expand=YES, padx=10, pady=(0, 0))

    def preview_cell_zbiorcze(event):
        item = dzienna.identify_row(event.y)
        col = dzienna.identify_column(event.x)
        if not item or not col:
            return
        col_num = int(col.replace("#", "")) - 1
        values = dzienna.item(item, "values")
        if col_num >= len(values):
            return
        tekst = str(values[col_num])
        if dzienne_columns[col_num].upper() in ("OPIS", "ZADANIE", "BIEŻĄCE ZADANIE"):
            win = tk.Toplevel(dzienna)
            win.title("Podgląd")
            win.geometry("540x220")
            center_popup(win, dzienna.winfo_toplevel())
            tk.Label(win, text=dzienne_columns[col_num], font=("Segoe UI", 12, "bold")).pack(pady=(10, 2))
            text_widget = tk.Text(win, wrap="word", height=6, font=("Segoe UI", 10))
            text_widget.insert("1.0", tekst)
            text_widget.config(state="disabled")
            text_widget.pack(padx=18, pady=8, fill="both", expand=True)
            tk.Button(win, text="Zamknij", command=win.destroy).pack(pady=(0, 12))
            win.grab_set()
            return "break"

    dzienna.bind("<Double-1>", preview_cell_zbiorcze)

    def wylicz_ecp(lista_czasow_full, lista_deklarowanych, lista_prywatnych):
        def czas_w_sekundach(od, do):
            try:
                t1 = datetime.strptime(od, "%H:%M:%S")
                t2 = datetime.strptime(do, "%H:%M:%S")
                delta = (t2 - t1).total_seconds()
                if delta < 0:
                    delta += 24 * 3600
                return max(delta, 0)
            except Exception:
                return 0

        suma_deklarowana = sum(
            hms_to_seconds(dek) * int(ilosc or 0)
            for od, do, zad, dek, ilosc in lista_deklarowanych
            if od and dek and (do not in ('', 'None', None))
        )
        suma_rzeczywista = sum(
            czas_w_sekundach(od, do)
            for od, do, zad, dek, ilosc in lista_deklarowanych
            if od and do and dek and do not in ('', 'None', None)
        )

        # TU DODAJEMY: TRWAJĄCE PRZERWY!
        now = datetime.now().strftime("%H:%M:%S")
        suma_prywatna = 0
        for od, do in lista_prywatnych:
            if not od or od in ("None", "null", "NULL", ""):
                continue
            if not do or do in ("None", "null", "NULL", ""):
                do_czas = now
            else:
                do_czas = do
            try:
                t1 = datetime.strptime(od, "%H:%M:%S")
                t2 = datetime.strptime(do_czas, "%H:%M:%S")
                if t2 < t1:
                    t2 += timedelta(days=1)
                diff = t2 - t1
                if 0 <= diff.total_seconds() < 8*3600:
                    suma_prywatna += diff.total_seconds()
            except Exception:
                continue

        if suma_deklarowana > 0 and suma_rzeczywista > 0:
            ecp = (suma_deklarowana / suma_rzeczywista) * 100
        else:
            ecp = 100.0
        if suma_prywatna > 0 and suma_rzeczywista > 0:
            ecp = ecp - (suma_prywatna / suma_rzeczywista * 100)
        ecp = max(ecp, 0)
        return f"{ecp:.0f}%"

    def aktualizuj_tabele(*_):
        miesiac = miesiac_var.get()
        miesiac_num, rok = map(int, miesiac.split("."))
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT osoba, kraj, zadanie, data, czas_od, czas_do, opis, ilosc
            FROM raport_ecp
            WHERE MONTH(data)=%s AND YEAR(data)=%s
            ORDER BY osoba, data, czas_od
        """, (miesiac_num, rok))
        records = cur.fetchall()
        daty = sorted({r[3].strftime("%d.%m.%Y") for r in records if r[3]})
        dni_combo['values'] = daty
        if not dni_var.get() or dni_var.get() not in daty:
            dni_var.set(daty[-1] if daty else "")
        conn.close()

        # MIESIĘCZNE
        podsumowanie = {}
        for osoba, kraj, zadanie, data, czas_od, czas_do, opis, ilosc in records:
            if osoba not in podsumowanie:
                podsumowanie[osoba] = []
            podsumowanie[osoba].append((str(czas_od), str(czas_do)))
        miesieczna.delete(*miesieczna.get_children())
        for osoba, czasy in podsumowanie.items():
            if osoba not in USERS or (USERS[osoba]["role"] != "user" and osoba != "dzial_techniczny_3"):
                continue
            godziny = sumuj_czasy(czasy)
            display_name = USER_TO_NAME.get(osoba, osoba)
            miesieczna.insert('', 'end', values=("KIELCE", "Dział BOT", display_name, godziny))

        # DZIENNE
        dzienna.delete(*dzienna.get_children())
        dzien = dni_var.get()
        czasy_pracy = {}
        prywatne = {}
        for osoba, kraj, zadanie, data, czas_od, czas_do, opis, ilosc in records:
            if osoba not in USERS or (USERS[osoba]["role"] != "user" and osoba != "dzial_techniczny_3"):
                continue
            if data and data.strftime("%d.%m.%Y") == dzien:
                key = osoba
                czasy_pracy.setdefault(key, []).append((str(czas_od), str(czas_do), kraj, zadanie, ilosc))
                if "przerwa prywatna" in (zadanie or "").lower():
                    prywatne.setdefault(key, []).append((str(czas_od), str(czas_do)))

        for osoba in czasy_pracy:
            display_name = USER_TO_NAME.get(osoba, osoba)
            lista_czasow = czasy_pracy[osoba]
            lista_czasow_full = []
            lista_deklarowanych = []
            lista_prywatnych = []
            for od, do,kraj, zad, ilosc in lista_czasow:
                deklarowany = CZASY_DEKLAROWANE.get(zad, {}).get(kraj, "")
                lista_czasow_full.append((od, do, zad, deklarowany, ilosc))
                if deklarowany:
                    lista_deklarowanych.append((od, do, zad, deklarowany, ilosc))
                if zad and "przerwa prywatna" in zad.lower():
                    lista_prywatnych.append((od, do))

            # NOWE LICZENIE ECP
            ecp_str = wylicz_ecp(lista_czasow_full, lista_deklarowanych, lista_prywatnych)

            # Reszta bez zmian (przerwa prywatna, szara strefa, itp.)
            prywatne_lista = [x for x in lista_prywatnych]
            suma_prywatna = timedelta()
            now = datetime.now().strftime("%H:%M:%S")
            dzisiaj_str = dni_var.get()

            for od, do in prywatne_lista:
                if not od or od in ("None", "null", "NULL", ""):
                    continue
                if not do or do in ("None", "null", "NULL", ""):
                    if dzisiaj_str == datetime.now().strftime("%d.%m.%Y"):
                        do_czas = now
                    else:
                        continue
                else:
                    do_czas = do
                try:
                    t1 = datetime.strptime(od, "%H:%M:%S")
                    t2 = datetime.strptime(do_czas, "%H:%M:%S")
                    if t2 < t1:
                        t2 += timedelta(days=1)
                    diff = t2 - t1
                    if 0 <= diff.total_seconds() < 8*3600:
                        suma_prywatna += diff
                except Exception:
                    continue
            sum_prywatne = seconds_to_hms(suma_prywatna.total_seconds())

            # --- Rzeczywiste zadania (bez prywaty) ---
            realne_zadania = []
            for od, do, zad, *_ in lista_czasow:
                if not od or "przerwa" in (zad or "").lower():
                    continue
                if (not do or str(do).lower() in ("none", "null", "")):
                    realne_zadania.append((od, datetime.now().strftime("%H:%M:%S")))
                else:
                    realne_zadania.append((od, do))
            suma = sumuj_czasy(realne_zadania)
            # --- Wszystkie bloki zadaniowe (nieprywatne + prywatne) ---
            wszystkie_bloki = []
            for blok in lista_czasow:
                od = blok[0]
                do = blok[1]
                if not od or od in ("None", "null", "NULL", ""):
                    continue
                if not do or do in ("None", "null", "NULL", ""):
                    wszystkie_bloki.append((od, datetime.now().strftime("%H:%M:%S")))
                else:
                    wszystkie_bloki.append((od, do))
            wszystkie_bloki_sorted = sorted(wszystkie_bloki, key=lambda x: x[0])
            szara_td = timedelta()
            for i in range(1, len(wszystkie_bloki_sorted)):
                od_prev, do_prev = wszystkie_bloki_sorted[i-1]
                od_curr, _ = wszystkie_bloki_sorted[i]
                if not do_prev or do_prev in ("None", "null", "NULL", ""):
                    continue
                try:
                    t_do = datetime.strptime(do_prev, "%H:%M:%S")
                    t_od = datetime.strptime(od_curr, "%H:%M:%S")
                    diff = t_od - t_do
                    if diff.total_seconds() > 0:
                        szara_td += diff
                except Exception:
                    continue

            laczny_czas_td = sumuj_czasy_td(wszystkie_bloki)
            laczny_czas = seconds_to_hms(laczny_czas_td.total_seconds())
            szara_str = seconds_to_hms(szara_td.total_seconds())
            laczny_czas_plus_szara = seconds_to_hms(laczny_czas_td.total_seconds() + szara_td.total_seconds())

            try:
                conn2 = connect_db()
                cur2 = conn2.cursor()
                data_mysql = datetime.strptime(dzisiaj_str, "%d.%m.%Y").strftime("%Y-%m-%d")
                cur2.execute("SELECT MIN(czas_od), MAX(czas_do) FROM raport_zmiany WHERE osoba=%s AND data=%s", (osoba, data_mysql))
                zm_start, zm_koniec = cur2.fetchone()
                conn2.close()
            except Exception:
                zm_start, zm_koniec = "", ""

            czas_start = zm_start if zm_start else (realne_zadania[0][0] if realne_zadania else "brak")
            czas_koniec = zm_koniec if zm_koniec else "brak"
            if not czas_start or str(czas_start).lower() == "none":
                czas_start = "brak"
            if not czas_koniec or str(czas_koniec).lower() == "none":
                czas_koniec = "brak"

            current_task = ""
            opis_value = ""
            kraj_value = ""    # Dodaj tę zmienną

            otwarte = [
                r for r in records
                if r[0] == osoba and r[3].strftime("%d.%m.%Y") == dzisiaj_str
                and r[4] and not r[5]
            ]
            if otwarte:
                ostatnie = otwarte[-1]
                current_task = ostatnie[2]
                opis_value = ostatnie[6] if ostatnie[6] else ""
                kraj_value = ostatnie[1] if ostatnie[1] not in (None, "", "None", "null", "NULL") else "-"
            else:
                current_task = "Koniec pracy" if czas_koniec != "brak" else "Brak zadań w toku"
                # Jeśli nie ma otwartego zadania i jest koniec pracy — kraj ma być pusty
                if czas_koniec != "brak":
                    kraj_value = ""
                else:
                    kraj_value = "-"
                    
            suma_s = hms_to_seconds(suma)
            szara_s = hms_to_seconds(szara_str)
            prywatne_s = hms_to_seconds(sum_prywatne)
            laczny_s = hms_to_seconds(laczny_czas)

            czas_zadan_plus_szara = seconds_to_hms(suma_s + szara_s)
            laczny_czas_plus_szara = seconds_to_hms(laczny_s + szara_s)

            dzienna.insert('', 'end', values=(
                dzisiaj_str,
                display_name,
                czas_start,
                czas_koniec,
                czas_zadan_plus_szara,
                laczny_czas_plus_szara,    # <-- poprawna nazwa, wyświetlaj to
                sum_prywatne,
                ecp_str,
                szara_str,
                kraj_value,
                current_task,
                opis_value
            ))
    def auto_update():
        aktualizuj_tabele()
        frame.after(30000, auto_update)  # co 30 sekund

    auto_update()

    miesiac_menu.bind("<<ComboboxSelected>>", aktualizuj_tabele)
    dni_combo.bind("<<ComboboxSelected>>", aktualizuj_tabele)
    aktualizuj_tabele()
    return frame

def open_main_panel(username, is_admin=False):
    main = tb.Toplevel()
    main.title("Kontrola czasu pracy - Panel Główny")
    main.geometry("1720x900")
    main.resizable(True, True)
    # root.iconbitmap(resource_path("ecp_icon.ico"))

    if is_admin:
        main_panel = tb.Frame(main)
        main_panel.pack(fill=BOTH, expand=YES)

        left_frame = tb.Frame(main_panel, width=260)
        left_frame.pack(side=LEFT, fill=Y, padx=6, pady=4)

        # Nagłówek „Użytkownicy:”
        tb.Label(left_frame, text="Użytkownicy:", font=("Segoe UI", 14, "bold")).pack(pady=(12, 6), anchor="w")

        user_tree = tk.ttk.Treeview(left_frame, show="tree", selectmode="browse")
        user_tree.pack(fill="y", expand=True)
        group_bot = user_tree.insert("", "end", text="Dział BOT", open=True)
        for user, conf in USERS.items():
            if conf["role"] == "user" and user.startswith("dzial_techniczny"):
                user_tree.insert(group_bot, "end", text=user, values=(user,), tags=('inactive_user',))

        style = ttk.Style()
        style.configure("Treeview", highlightthickness=0, bd=0, font=('Segoe UI', 9))
        style.layout("Treeview", [('Treeview.treearea', {'sticky': 'nswe'})])
        style.map('Treeview', background=[('selected', '#4ba0e3')])

        btn_kontrola = tb.Button(left_frame, text="Podział procentowy", bootstyle="info",
                                 command=lambda: load_panel("procent"))
        btn_kontrola.pack(side="bottom", fill="x", padx=8, pady=(2, 4))

        btn_zbiorcze = tb.Button(left_frame, text="Informacje zbiorcze", bootstyle="info",
                                 command=lambda: load_panel("zbiorcze"))
        btn_zbiorcze.pack(side="bottom", fill="x", padx=8, pady=(2, 12))

        def import_csv_to_db():
            file_path = filedialog.askopenfilename(
                filetypes=[("CSV files", "*.csv")],
                title="Wybierz plik CSV do importu"
            )
            if not file_path:
                return
            imported = 0
            with open(file_path, encoding="utf-8-sig") as csvfile:
                reader = csv.DictReader(csvfile, delimiter=";")
                for row in reader:
                    print("ROW:", row) 
                    data_mysql = row.get("DATA") or row.get("data")
                    osoba = row.get("OSOBA") or row.get("osoba")
                    kraj = row.get("KRAJ") or row.get("kraj")
                    zadanie = row.get("ZADANIE") or row.get("zadanie")
                    opis = row.get("OPIS") or row.get("opis")
                    ilosc = row.get("ILOŚĆ") or row.get("ilosc") or "0"
                    czas_od = row.get("CZAS OD") or row.get("czas_od") or None
                    czas_do = row.get("CZAS DO") or row.get("czas_do") or None
                    if data_mysql and "." in data_mysql:
                        try:
                            data_mysql = datetime.strptime(data_mysql, "%d.%m.%Y").strftime("%Y-%m-%d")
                        except Exception:
                            continue
                    try:
                        ilosc_int = int(ilosc)
                    except Exception:
                        ilosc_int = 0
                    row_db = (
                        data_mysql,
                        osoba,
                        kraj,
                        zadanie,
                        opis,
                        ilosc_int,
                        czas_od,
                        czas_do
                    )
                    try:
                        insert_record(row_db)
                        imported += 1
                    except Exception as ex:
                        print("BŁĄD importu:", ex, row)
            messagebox.showinfo("Import CSV", f"Zaimportowano {imported} wierszy z historii.")

        btn_import = tb.Button(left_frame, text="Importuj historię (CSV)", bootstyle="primary", command=import_csv_to_db)
        btn_import.pack(side="bottom", fill="x", padx=8, pady=(2, 12))

        right_frame = tb.Frame(main_panel)
        right_frame.pack(side=LEFT, fill=BOTH, expand=YES)

        current_panel = {'frame': None, 'nick': None}

        def clear_all_tags(tree):
            def clear(iid):
                tree.item(iid, tags=())
                for child in tree.get_children(iid):
                    clear(child)
            for iid in tree.get_children():
                clear(iid)

        def on_hover(event):
            row_id = user_tree.identify_row(event.y)
            clear_all_tags(user_tree)
            if row_id:
                user_tree.item(row_id, tags=('hover',))

        user_tree.tag_configure('hover', background='#2a4365')
        user_tree.bind('<Motion>', on_hover)
        user_tree.bind('<Leave>', lambda e: clear_all_tags(user_tree))

        def load_panel(panel_type="procent", nick=None):
            if current_panel['frame']:
                current_panel['frame'].destroy()
                current_panel['frame'] = None
            if panel_type == "procent":
                current_panel['frame'] = panel_procentowy(right_frame)
                current_panel['nick'] = None
            elif panel_type == "ecp" and nick:
                current_panel['frame'] = panel_raport_ecp(right_frame, username=nick, is_admin=True)
                current_panel['nick'] = nick
            elif panel_type == "zbiorcze":
                current_panel['frame'] = panel_informacje_zbiorcze(right_frame)
                current_panel['nick'] = None

        def on_tree_click(event):
            selected = user_tree.selection()
            if not selected:
                return
            item = selected[0]
            parent = user_tree.parent(item)
            if parent == "":
                load_panel("procent")
                return
            user_login = user_tree.item(item, "values")
            if user_login and user_login[0] in USERS:
                load_panel("ecp", user_login[0])

        user_tree.bind("<<TreeviewSelect>>", on_tree_click)
        load_panel("procent")

    else:
        # --- PANEL USERA Z DRZEWKIEM I INF. ZBIORCZĄ ---
        main_panel = tb.Frame(main)
        main_panel.pack(fill=BOTH, expand=YES)

        left_frame = tb.Frame(main_panel, width=260)
        left_frame.pack(side=LEFT, fill=Y, padx=6, pady=4)

        tb.Label(left_frame, text="Użytkownicy:", font=("Segoe UI", 14, "bold")).pack(pady=(12, 6), anchor="w")
        user_tree = tk.ttk.Treeview(left_frame, show="tree", selectmode="browse")
        user_tree.pack(fill="y", expand=True)
        group_bot = user_tree.insert("", "end", text="Dział BOT", open=True)

        # Najpierw wstawia zalogowanego usera (nick na górze!)
        user_iid = user_tree.insert(group_bot, "end", text=username, values=(username,), tags=('self_user',))
        for user, conf in USERS.items():
            if user == username:
                continue
            if conf["role"] == "user" and user.startswith("dzial_techniczny"):
                user_tree.insert(group_bot, "end", text=user, values=(user,), tags=('inactive_user',))

        style = ttk.Style()
        style.configure("Treeview", highlightthickness=0, bd=0, font=('Segoe UI', 9))
        style.layout("Treeview", [('Treeview.treearea', {'sticky': 'nswe'})])
        user_tree.tag_configure('inactive_user', foreground='#a0a0a0')

        btn_zbiorcze = tb.Button(left_frame, text="Informacje zbiorcze", bootstyle="info",
                                command=lambda: load_panel("zbiorcze"))
        btn_zbiorcze.pack(side="bottom", fill="x", padx=8, pady=(2, 12))

        right_frame = tb.Frame(main_panel)
        right_frame.pack(side=LEFT, fill=BOTH, expand=YES)

        current_panel = {'frame': None, 'nick': None}

        def load_panel(panel_type="ecp", nick=None):
            if current_panel['frame']:
                current_panel['frame'].destroy()
                current_panel['frame'] = None
            if panel_type == "ecp":
                current_panel['frame'] = panel_raport_ecp(right_frame, username=username, is_admin=False)
                current_panel['nick'] = username
            elif panel_type == "zbiorcze":
                current_panel['frame'] = panel_informacje_zbiorcze(right_frame)
                current_panel['nick'] = None

        def on_tree_click(event):
            selected = user_tree.selection()
            if not selected:
                return
            item = selected[0]
            tags = user_tree.item(item, 'tags')
            user_login = user_tree.item(item, "values")
            # Pozwalaj kliknąć TYLKO na swój nick (niezależnie czy już jest aktywny)
            if 'self_user' in tags and user_login and user_login[0] == username:
                load_panel("ecp", username)
            else:
                user_tree.selection_remove(item)  # nie pozwalaj na klik innych

        user_tree.bind("<<TreeviewSelect>>", on_tree_click)

        # Po zalogowaniu podświetl swojego usera!
        user_tree.selection_set(user_iid)
        user_tree.focus(user_iid)
        user_tree.see(user_iid)

        load_panel("ecp", username)

# --- LOGOWANIE ---
if getattr(sys, "frozen", False):
    exe_path = Path(sys.executable).resolve()
    remove_old_versions(exe_path)
check_for_update()
root = tb.Window(themename="superhero")
root.title("Kontrola czasu pracy")
root.geometry("600x400")
root.resizable(True, True)
# root.iconbitmap(resource_path("ecp_icon.ico"))
main_title = tb.Label(
    root,
    text="Kontrola czasu pracy",
    font=("Segoe UI", 18, "bold"),
    foreground="#fff",
    background=root.cget("background"),
    wraplength=580,
    anchor="center"
)
main_title.place(relx=0.5, rely=0.13, anchor="center")
container = tb.Frame(root)
container.place(relx=0.5, rely=0.48, anchor="center")
tb.Label(container, text="Login:", font=("Segoe UI", 12)).grid(row=0, column=0, sticky=E, pady=10, padx=(0, 12))
entry_user = tb.Entry(container, font=("Segoe UI", 10), width=30)
entry_user.grid(row=0, column=1, pady=10)
tb.Label(container, text="Hasło:", font=("Segoe UI", 12)).grid(row=1, column=0, sticky=E, pady=10, padx=(0, 12))
entry_pass = tb.Entry(container, show="*", font=("Segoe UI", 10), width=30)
entry_pass.grid(row=1, column=1, pady=10)
def login():
    user = entry_user.get().strip().lower()
    pwd = entry_pass.get()
    if user in USERS and USERS[user]["password"] == pwd:
        role = USERS[user]["role"]
        root.withdraw()
        open_main_panel(user, is_admin=(role == "admin"))
    else:
        messagebox.showerror("Błąd", "Nieprawidłowy login lub hasło.")
login_btn = tb.Button(container, text="Zaloguj się", bootstyle=SUCCESS, width=32, command=login)
login_btn.grid(row=2, column=0, columnspan=2, pady=28)
date_label = tb.Label(root, text="Copyright © 2024 Kontrol Panel", font=("Segoe UI", 10, "italic"))
date_label.place(relx=1.0, rely=1.0, x=-20, y=-18, anchor="se")
root.mainloop()
