import matplotlib.pyplot as plt


def plot_revenue_by_month_year(df):
    """
    Plot revenue per month for years 2016, 2017, and 2018.
    Expects columns: 'month', 'Year2016', 'Year2017', 'Year2018'
    """
    plt.figure()
    plt.plot(df['month'], df['Year2016'], label='2016')
    plt.plot(df['month'], df['Year2017'], label='2017')
    plt.plot(df['month'], df['Year2018'], label='2018')
    plt.xlabel('Month')
    plt.ylabel('Revenue')
    plt.title('Revenue by Month and Year')
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_real_vs_estimated(df):
    """
    Plot real vs estimated delivery times by month for years 2016–2018.
    Expects columns:
    'month', 'Year2016_real_time', 'Year2017_real_time', 'Year2018_real_time',
    'Year2016_estimated_time', 'Year2017_estimated_time', 'Year2018_estimated_time'
    """
    plt.figure()
    plt.plot(df['month'], df['Year2016_real_time'], label='2016 Real')
    plt.plot(df['month'], df['Year2017_real_time'], label='2017 Real')
    plt.plot(df['month'], df['Year2018_real_time'], label='2018 Real')
    plt.plot(df['month'], df['Year2016_estimated_time'], label='2016 Estimated')
    plt.plot(df['month'], df['Year2017_estimated_time'], label='2017 Estimated')
    plt.plot(df['month'], df['Year2018_estimated_time'], label='2018 Estimated')
    plt.xlabel('Month')
    plt.ylabel('Days')
    plt.title('Real vs Estimated Delivery Times')
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_orders_per_day_and_holidays_2017(df):
    """
    Plot number of orders per day in 2017, highlighting holidays.
    Expects columns: 'date', 'orders', 'is_holiday'
    """
    plt.figure()
    plt.plot(df['date'], df['orders'], label='Orders')
    holiday_dates = df[df['is_holiday'] == 1]['date']
    holiday_orders = df[df['is_holiday'] == 1]['orders']
    plt.scatter(holiday_dates, holiday_orders, color='red', label='Holiday')
    plt.xlabel('Date')
    plt.ylabel('Orders')
    plt.title('Orders per Day in 2017 (Holidays Highlighted)')
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_freight_vs_weight(df):
    """
    Scatter plot of freight value vs total weight.
    Expects columns: 'total_weight_g', 'total_freight_value'
    """
    plt.figure()
    plt.scatter(df['total_weight_g'], df['total_freight_value'])
    plt.xlabel('Total Weight (g)')
    plt.ylabel('Total Freight Value')
    plt.title('Freight Value vs Weight')
    plt.tight_layout()
    plt.show()
