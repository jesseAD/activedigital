from datetime import datetime, timedelta, timezone
from dateutil import tz, relativedelta
from dateutil.relativedelta import FR


def get_prompt(expiry_date, date):
  if not isinstance(expiry_date, datetime) or not isinstance(date, datetime):
    return ""
  
  prompts = ["THISWEEK", "NEXTWEEK", "THISMONTH", "NEXTMONTH", "QUARTER", "NEXTQUARTER"]
  for item in prompts:
    if expiry_date <= get_expiry_date(item, date):
      prompt = item
      break

  return prompt

def get_expiry_date(prompt, date):
  expiry_date = date

  if prompt == "THISWEEK":
    expiry_date = get_next_friday(date)
  elif prompt == "NEXTWEEK":
    expiry_date = get_friday_after_next(date)
  elif prompt == "THISMONTH":
    expiry_date = get_last_friday_this_month(date)
  elif prompt == "NEXTMONTH":
    expiry_date = get_last_friday_next_month(date)
  elif prompt == "QUARTER":
    expiry_date = get_last_friday_of_quarter(date)
  elif prompt == "NEXTQUARTER":
    expiry_date = get_last_friday_of_next_quarter(date)

  return expiry_date

def get_prompt_month_code_from_expiry_date(expiry_date):
  month_codes = " FGHJKMNQUVXZ"

  return month_codes[expiry_date.month] + expiry_date.strftime('%y')

def get_expiry_date_from_prompt_month_code(prompt_month_code):
  month_codes = " FGHJKMNQUVXZ"

  month = month_codes.index(prompt_month_code[0])
  day = int(prompt_month_code[1:])
  year = datetime.now().year if month >= datetime.now().month else datetime.now().year - 1

  return datetime(year=year, month=month, day=day, hour=0, minute=0, second=0).replace(tzinfo=timezone.utc)

def get_last_friday_of_next_quarter(date):
    local_zone = date.tzinfo
    target_zone = tz.gettz('Hongkong')

    date_hk = date.astimezone(target_zone)
    date_hk = date_hk.replace(hour=16, minute=0)

    quarter = (date_hk.month - 1) // 3 + 1

    next_quarter = (quarter % 4) + 1

    start_next_quarter = date_hk.replace(month=3*(next_quarter-1) + 3, day=1)
    
    end_next_quarter = start_next_quarter + relativedelta.relativedelta(day=1, months=3)
    
    last_friday_next_quarter = end_next_quarter + relativedelta.relativedelta(day=31, weekday=FR(-1))
    
    local_time = last_friday_next_quarter.astimezone(local_zone)
    
    last_friday_quarter = get_last_friday_of_quarter(date)
    
    diff = (local_time - last_friday_quarter).days

    if diff <= 0:
        local_time = get_last_friday_of_next_quarter(end_next_quarter)
    
    return local_time

def get_last_friday_of_quarter(date):
  local_zone = date.tzinfo
  target_zone = tz.gettz('Hongkong')

  date_hk = date.astimezone(target_zone)
  date_hk = date_hk.replace(hour=16, minute=0)

  quarter = (date_hk.month - 1) // 3 + 1
  
  if quarter == 1:
    end_month = 3
  elif quarter == 2:
    end_month = 6
  elif quarter == 3:
    end_month = 9
  else:
    end_month = 12
  
  end_quarter = date_hk.replace(month=end_month, day=1)
  
  last_friday_quarter = end_quarter + relativedelta.relativedelta(day=31, weekday=FR(-1))
  local_time = last_friday_quarter.astimezone(local_zone)
  
  last_friday_month = get_last_friday_this_month(date)
  
  diff = (last_friday_quarter - last_friday_month).days

  if diff <= 0:
    next_month = date + relativedelta.relativedelta(day=1, months=1)
    local_time = get_last_friday_of_quarter(next_month)
  
  return local_time

def get_next_friday(date):
  local_zone = date.tzinfo
  target_zone = tz.gettz('Hongkong')
  
  date_hk = date.astimezone(target_zone)
  date_hk = date_hk.replace(hour=16, minute=0)
  
  if date_hk.weekday() == 4 and date.astimezone(target_zone).hour < 16: # Friday corresponds to 4 in Python
    next_friday = date_hk

  elif date_hk.weekday() == 4:
    next_friday = date_hk + timedelta(days=7)
  else:
    next_friday = date_hk + timedelta((4 - date_hk.weekday() + 7) % 7)
      
  local_time = next_friday.astimezone(local_zone)
  
  return local_time

def get_last_friday_this_month(date):
  local_zone = date.tzinfo
  target_zone = tz.gettz('Hongkong')

  date_hk = date.astimezone(target_zone)
  date_hk = date_hk.replace(hour=16, minute=0)

  last_friday_this_month = date_hk + relativedelta.relativedelta(day=31, weekday=FR(-1))

  local_time = last_friday_this_month.astimezone(local_zone)

  last_friday_next_week = get_friday_after_next(date)
  diff = (last_friday_this_month - last_friday_next_week).days

  if diff <= 0:
    next_month = last_friday_this_month + relativedelta.relativedelta(day=1, months=1)
    local_time = get_last_friday_this_month(next_month)

  return local_time

def get_last_friday_next_month(date):
  local_zone = date.tzinfo
  target_zone = tz.gettz('Hongkong')

  date_hk = date.astimezone(target_zone)
  date_hk = date_hk.replace(hour=16, minute=0)

  last_friday_next_month = date_hk + relativedelta.relativedelta(months=1, day=31, weekday=FR(-1)) 

  local_time = last_friday_next_month.astimezone(local_zone)

  last_friday_this_month = get_last_friday_this_month(date)
  diff = (last_friday_next_month - last_friday_this_month).days

  if diff <= 0:
    next_month = last_friday_next_month + relativedelta.relativedelta(day=1, months=1)
    local_time = get_last_friday_this_month(next_month)

  return local_time

def get_this_friday(date):
  local_zone = date.tzinfo
  target_zone = tz.timezone('HongKong')
  
  date_hk = date.astimezone(target_zone)
  date_hk = date_hk.replace(hour=16, minute=0)
  
  if date_hk.weekday() == 4 and date.astimezone(target_zone).hour < 16: # Friday corresponds to 4 in Python
    this_friday = date_hk
  else:
    this_friday = date_hk + timedelta((5 - date_hk.weekday() + 7) % 7)
      
  local_time = this_friday.astimezone(local_zone)
  
  return local_time

def get_friday_after_next(date):
  local_zone = date.tzinfo
  target_zone = tz.gettz("Hongkong")
  
  date_hk = date.astimezone(target_zone)
  date_hk = date_hk.replace(hour=16, minute=0)

  next_friday = date_hk + timedelta((4 - date_hk.weekday() + 7) % 7)
  
  # if date_hk.weekday() == 4 and date.astimezone(target_zone).hour < 16: # DayOfWeek.FRIDAY is 4 in Python
  #   next_friday = date_hk + timedelta((5 - date_hk.weekday() + 7) % 7)
  # else:
  #   next_friday = date_hk + timedelta((5 - date_hk.weekday() + 7) % 7)
  #   next_friday = next_friday + timedelta((5 - next_friday.weekday() + 7) % 7)

  if date_hk.weekday() == 4 and date.astimezone(target_zone).hour < 16:
    next_friday += timedelta(days=7)
  elif date_hk.weekday() == 4:
    next_friday += timedelta(days=14)
  else:
    next_friday += timedelta(days=7)
                    
  local_time = next_friday.astimezone(local_zone)

  return local_time


# date = datetime.now(timezone.utc)
# print(get_expiry_date_from_prompt_month_code("H25"))
# print(get_expiry_date("THISWEEK", date))
# print(get_expiry_date("NEXTWEEK", date))
# print(get_expiry_date("THISMONTH", date))
# print(get_expiry_date("NEXTMONTH", date))
# print(get_expiry_date("QUARTER", date))
# print(get_expiry_date("NEXTQUARTER", date))