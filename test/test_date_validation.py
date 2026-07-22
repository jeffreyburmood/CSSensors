from datetime import datetime, timedelta


def is_today_or_yesterday(date_str: str) -> bool:
    try:
        input_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        return False

    today = datetime.today().date()
    yesterday = today - timedelta(days=1)

    return input_date in (today, yesterday)

def main() -> None:
    date1 = '2026-07-22'
    print(f'Validation result for {date1} is {is_today_or_yesterday(date1)}')
    date2 = '2026-07-21'
    print(f'Validation result for {date2} is {is_today_or_yesterday(date2)}')
    date3 = '2026-07-23'
    print(f'Validation result for {date3} is {is_today_or_yesterday(date3)}')
    date4 = '2025-07-22'
    print(f'Validation result for {date4} is {is_today_or_yesterday(date4)}')

if __name__ == "__main__":
    try:
        main()

    except KeyboardInterrupt:
        print("Received KeyboardInterrupt, shutting down.")
