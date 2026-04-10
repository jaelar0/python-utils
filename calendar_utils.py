import pandas as pd

# Holiday data (US Federal - observed dates)
data = [
    # 2026
    {"Year": 2026, "Holiday": "New Year's Day", "Date": "2026-01-01"},
    {"Year": 2026, "Holiday": "MLK Jr. Day", "Date": "2026-01-19"},
    {"Year": 2026, "Holiday": "Washington's Birthday", "Date": "2026-02-16"},
    {"Year": 2026, "Holiday": "Memorial Day", "Date": "2026-05-25"},
    {"Year": 2026, "Holiday": "Juneteenth", "Date": "2026-06-19"},
    {"Year": 2026, "Holiday": "Independence Day (Observed)", "Date": "2026-07-03"},
    {"Year": 2026, "Holiday": "Labor Day", "Date": "2026-09-07"},
    {"Year": 2026, "Holiday": "Columbus Day", "Date": "2026-10-12"},
    {"Year": 2026, "Holiday": "Veterans Day", "Date": "2026-11-11"},
    {"Year": 2026, "Holiday": "Thanksgiving", "Date": "2026-11-26"},
    {"Year": 2026, "Holiday": "Christmas Day", "Date": "2026-12-25"},

    # 2027
    {"Year": 2027, "Holiday": "New Year's Day", "Date": "2027-01-01"},
    {"Year": 2027, "Holiday": "MLK Jr. Day", "Date": "2027-01-18"},
    {"Year": 2027, "Holiday": "Washington's Birthday", "Date": "2027-02-15"},
    {"Year": 2027, "Holiday": "Memorial Day", "Date": "2027-05-31"},
    {"Year": 2027, "Holiday": "Juneteenth (Observed)", "Date": "2027-06-18"},
    {"Year": 2027, "Holiday": "Independence Day (Observed)", "Date": "2027-07-05"},
    {"Year": 2027, "Holiday": "Labor Day", "Date": "2027-09-06"},
    {"Year": 2027, "Holiday": "Columbus Day", "Date": "2027-10-11"},
    {"Year": 2027, "Holiday": "Veterans Day", "Date": "2027-11-11"},
    {"Year": 2027, "Holiday": "Thanksgiving", "Date": "2027-11-25"},
    {"Year": 2027, "Holiday": "Christmas Day (Observed)", "Date": "2027-12-24"},
]

# Create dataframe
df = pd.DataFrame(data)

# Convert to datetime
df["Date"] = pd.to_datetime(df["Date"])

# Add weekday
df["Weekday"] = df["Date"].dt.day_name()

# Flag if between Monday–Saturday
df["Monday_to_Saturday"] = df["Weekday"].isin(
    ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
)

# Optional flags
df["Is_Monday"] = df["Weekday"] == "Monday"
df["Is_Saturday"] = df["Weekday"] == "Saturday"

print(df)
