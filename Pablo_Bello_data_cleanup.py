import pandas as pd

booking = pd.read_csv('booking_logs.csv')
cancel = pd.read_csv('cancel_logs.csv')
shift = pd.read_csv('cleveland_shifts.csv')

# let's clean up this data so that only booking and cancellation data for the shifts included
booking = booking[booking['Shift ID'].isin(shift['ID'])]
cancel = cancel[cancel['Shift ID'].isin(shift['ID'])]

# then we clean up all the dates to datetime


def convert_datetime(table, column_list):
    for column in column_list:
        table[column] = pd.to_datetime(table[column], format='mixed')
    return table


booking = convert_datetime(booking, ['Created At'])
cancel = convert_datetime(cancel, ['Created At', 'Shift Start Logs'])
shift = convert_datetime(shift, ['Start', 'End', 'Created At'])

# add lead time for shifts when they are made and change all IDs to appropriate terminology (e.g. Shift ID)
shift['Shift Lead Time'] = (shift['Start'] - shift['Created At']).dt.total_seconds() / 3600


def column_name_change(table, column_change):
    # Where column_change is [ [a,b], [c,d], ...]
    for n in column_change:
        table.rename(columns={n[0]: n[1]}, inplace=True)


shift_change = [
    ['ID', 'Shift ID'],
    ['Created At', 'Shift Created At']
]

booking_change = [
    ['ID', 'Booking ID'],
    ['Created At', 'Booking Created At'],
    ['Action', 'Booking Action'],
    ['Lead Time', 'Booking Lead Time']
]

cancel_change = [
    ['ID', 'Cancel ID'],
    ['Created At', 'Cancel Created At'],
    ['Action', 'Cancel Action'],
    ['Lead Time', 'Cancel Lead Time']
]

column_name_change(shift, shift_change)
column_name_change(booking, booking_change)
column_name_change(cancel, cancel_change)


# Now let's look at the first booking for a shift to understand booking rate by time

def filter_unique_first(table, column):
    table = table.sort_values(column)
    table = table.drop_duplicates(subset=table.columns.difference([column]), keep='first')
    return table


booking_shift_first = filter_unique_first(booking_shift, 'Created At_x')

shift_booking_first = pd.merge(shift, booking_shift_first[[
                               'Shift ID', 'Booking Lead Time', 'Booking ID']], on='Shift ID', how='left')

# shift_booking_first.to_csv('booking_first.csv',index=False)
# print(booking_shift_first.head())

# To find cancel rate by user cohort and lead time of booking (or shift), we first need to match bookings to cancellations.
# If I had more time I would write a script that handles mutliple cancellations on the same shift from the same user (likely only a few cases of this)

booking_cancel = pd.merge(booking, cancel, on=['Shift ID', 'Worker ID'], how='left')

# print(booking_cancel.head())
# booking_cancel.to_csv('booking_cancel.csv',index=False)

# Now I'm going to build a grouping functions that lets me put in a field I want to group by and see relevant cancellation and booking stats.


def cancel_group_by(table, field):
    # this function will group by a certain field (e.g. Worker ID) and return a collection of relevant metrics
    new_table = table.groupby(field).agg({'Cancel Lead Time': ['mean', 'count'],
                                          'Booking ID': 'count',
                                          'Cancel Action': [lambda x: (x == 'NO_CALL_NO_SHOW').sum(),
                                                            lambda x: ((x == 'WORKER_CANCEL') & (
                                                                booking_cancel['Cancel Lead Time'] < 4)).sum(),
                                                            lambda x: ((x == 'WORKER_CANCEL') & (booking_cancel['Cancel Lead Time'] >= 4) & (
                                                                booking_cancel['Cancel Lead Time'] < 24)).sum(),
                                                            lambda x: ((x == 'WORKER_CANCEL') & (
                                                                booking_cancel['Cancel Lead Time'] >= 24)).sum()
                                                            ]
                                          }).reset_index()
    new_table.columns = [field, 'Avg Cancel Lead Time', 'Cancel Count', 'Booking Count',
                         'Cancel No Call', 'Cancel Call Off', 'Cancel 24', 'Cancel Standard']
    return new_table

# Now we create a bunch of csv's cause the graphing function in pandas isn't super intutitive and I can do this 20x faster in excel/sheets.


worker = booking_cancel.groupby('Worker ID').agg({'Cancel Lead Time': ['mean', 'count'],
                                                  'Booking ID': 'count',
                                                  'Cancel Action': [lambda x: (x == 'NO_CALL_NO_SHOW').sum(),
                                                                    lambda x: ((x == 'WORKER_CANCEL') & (
                                                                        booking_cancel['Cancel Lead Time'] < 4)).sum(),
                                                                    lambda x: ((x == 'WORKER_CANCEL') & (booking_cancel['Cancel Lead Time'] >= 4) & (
                                                                        booking_cancel['Cancel Lead Time'] < 24)).sum(),
                                                                    lambda x: ((x == 'WORKER_CANCEL') & (
                                                                        booking_cancel['Cancel Lead Time'] >= 24)).sum()
                                                                    ]
                                                  }).reset_index()
worker.columns = ['Worker ID', 'Avg Cancel Lead Time', 'Cancel Count',
                  'Booking Count', 'Cancel No Call', 'Cancel Call Off', 'Cancel 24', 'Cancel Standard']

# print(worker.head())
# worker.to_csv('worker.csv',index=False)

facility = booking_cancel.groupby('Facility ID_x').agg({'Cancel Lead Time': 'count',
                                                        'Booking ID': 'count',
                                                        'Cancel Action': [lambda x: (x == 'NO_CALL_NO_SHOW').sum(),
                                                                          lambda x: ((x == 'WORKER_CANCEL') & (
                                                                              booking_cancel['Cancel Lead Time'] < 4)).sum(),
                                                                          lambda x: ((x == 'WORKER_CANCEL') & (booking_cancel['Cancel Lead Time'] >= 4) & (
                                                                              booking_cancel['Cancel Lead Time'] < 24)).sum(),
                                                                          lambda x: ((x == 'WORKER_CANCEL') & (
                                                                              booking_cancel['Cancel Lead Time'] >= 24)).sum()
                                                                          ]
                                                        }).reset_index()
facility.columns = ['Facility ID', 'Cancel Count', 'Booking Count',
                    'Cancel No Call', 'Cancel Call Off', 'Cancel 24', 'Cancel Standard']

# print(facility.head())
# facility.to_csv('facility.csv',index=False)

booking_detail = pd.merge(
    booking_cancel, shift[['Shift ID', 'Charge', 'Agent Req', 'Shift Type']], on=['Shift ID'], how='left')
print(booking_detail.head())

charge = cancel_group_by(booking_detail, 'Charge')
print(charge.head())
charge.to_csv('charge.csv', index=False)

agent_req = cancel_group_by(booking_detail, 'Agent Req')
print(agent_req.head())
agent_req.to_csv('agent_req.csv', index=False)

shift_type = cancel_group_by(booking_detail, 'Shift Type')
print(shift_type.head())
shift_type.to_csv('shift_type.csv', index=False)
