""" this file contains code to practice with parsing datetime objects """
import time
from datetime import datetime, date

from neo4j import GraphDatabase

my_datetime = datetime.strptime('2026-05-16 13:01:12', '%Y-%m-%d %H:%M:%S')  # change from datetime.now()
print(f'the my_datetime value is {my_datetime} is type {type(my_datetime)}')
my_date = my_datetime.date()
print(f'the my_date value is {my_date} is type {type(my_date)}')
my_time = my_datetime.time()
print(f'the my_time value is {my_time} is type {type(my_time)}')
# my_date_string = datetime.strptime('2026-05-16 13:01:12', '%Y-%m-%d %H:%M:%S')
# print(f'the my_date_string value is {my_date_string} is type {type(my_date_string)}')
year = my_date.year
month = my_date.month
day = my_date.day
hour = my_time.hour
print(f'the year = {year} is type {type(year)}')
print(f'the month = {month} is type {type(month)}')
print(f'the day = {day} is type {type(day)}')
print(f'the hour = {hour} is type {type(hour)}')

# set things up to add data to database
driver = GraphDatabase.driver("neo4j://localhost:7687")
driver.verify_connectivity()

year_name = my_date.strftime('%Y')
month_name = my_date.strftime('%Y-%m')
day_name = my_date.strftime('%Y-%m-%d')
hour_name = my_time.strftime('%H')
print(f'the year = {year_name} is type {type(year_name)}')
print(f'the month = {month_name} is type {type(month_name)}')
print(f'the day = {day_name} is type {type(day_name)}')
print(f'the hour = {hour_name} is type {type(hour_name)}')

time.sleep(3)

driver.close()

print('All Done!')


