from utils import *
from sql import *
from datetime import datetime, timedelta

ID = 'ID'
NAME = 'Name'
MOBILE_NUMBER = 'Mobile Number'
NIC = 'CNIC'
RIDER_CITY = 'Rider City'
EQUIPMENT_COST = 'Equipment Cost'
PICKUP_BONUS = 'Pickup Bonus (PB)'
PICKUP_DISTANCE = "Pickup Distance KM"
DROP_OFF_DISTANCE_PAY = 'Drop-off Pay (DDP)'
DROP_OFF_DISTANCE = "Drop-off Distance KM"
RECEIVABLE_AMOUNT = 'Receivable Amount'
FUEL_ALLOWANCE = "Fuel Allowance"
FINAL_RECEIVABLE_AMOUNT = "Final Receivable Amount"
LAST_SETTLEMENT_DATE = 'Last Settlement Date'
DATE_LAST_SETTLEMENT = 'Date'
TIME_LAST_SETTLEMENT = 'Time'


def rider_receivables(start_date, end_date):
    riders_data = []
    riders = get_data()

    for rider in riders:
        report_last_settlement = ''
        report_last_settlement_date = ''
        report_last_settlement_time = ''
        last_settlement = last_settlement_query(end_date, rider[0])


        if last_settlement:
            report_last_settlement = str(last_settlement[0][1])
            print('report',report_last_settlement)
            last_settlement_date = str(last_settlement[0][1].date())
            report_last_settlement_time = str(last_settlement[0][1].time())
            report_last_settlement_date = last_settlement_date
            # start and end times for filter earnings
            data = get_dates( last_settlement_date, end_date)
            start_time, end_time = report_last_settlement, data['end_time']
        else:
            config = logistics_configuration_instance()
            # start and end times for filter earnings
            data = get_dates( str(config[0]), end_date)
            start_time, end_time = data['start_time'], data['end_time']

        rc_sum = rc_sum_query(end_date, rider[0])
        print('rc_sum', rc_sum)
        trans_type_c = rc_sum[0][0] or 0
        trans_type_d = rc_sum[0][1] or 0
        sum_total = trans_type_c - trans_type_d
        receivable_amount = rider[1] - sum_total
        equipment_cost = get_equipment_cost(rider[0], start_time, end_time)
        print('equipment', equipment_cost)
        pickup_distances = get_rider_pickup_distances(rider[0], start_time,
                                                      end_time, log_type="PB")
        pickup_distance = float(pickup_distances)
        print('pickup', pickup_distance)
        delivered_distances = get_rider_drop_off_distances(rider[0], start_time,
                                                           end_time, log_type="DDP")
        delivered_distance = float(delivered_distances)
        print('del',delivered_distance)
        earnings_data = get_rider_earnings(rider[0], start_time, end_time)
        pick_up_distance_bonus = earnings_data[0][0] or 0
        drop_off_distance_pay = earnings_data[0][2] or 0
        print('dropoff', drop_off_distance_pay)
        fuel_allowance = pick_up_distance_bonus + drop_off_distance_pay
        final_receivable_amount = receivable_amount - fuel_allowance
        if final_receivable_amount >= 1:
            riders_data.append({ID: rider[0], NAME: rider[2], MOBILE_NUMBER: rider[4],
                                RIDER_CITY: rider[3], NIC: rider[5],
                                EQUIPMENT_COST: equipment_cost,
                                PICKUP_BONUS: pick_up_distance_bonus,
                                PICKUP_DISTANCE: pickup_distance,
                                DROP_OFF_DISTANCE_PAY: drop_off_distance_pay,
                                DROP_OFF_DISTANCE: delivered_distance,
                                RECEIVABLE_AMOUNT: receivable_amount,
                                FUEL_ALLOWANCE: fuel_allowance,
                                FINAL_RECEIVABLE_AMOUNT: final_receivable_amount,
                                LAST_SETTLEMENT_DATE: report_last_settlement,
                                DATE_LAST_SETTLEMENT: report_last_settlement_date,
                                TIME_LAST_SETTLEMENT: report_last_settlement_time
                                })
    header = [ID, NAME, MOBILE_NUMBER, NIC, RIDER_CITY, EQUIPMENT_COST, PICKUP_BONUS, PICKUP_DISTANCE,
              DROP_OFF_DISTANCE_PAY, DROP_OFF_DISTANCE, RECEIVABLE_AMOUNT, FUEL_ALLOWANCE, FINAL_RECEIVABLE_AMOUNT,
              LAST_SETTLEMENT_DATE, DATE_LAST_SETTLEMENT, TIME_LAST_SETTLEMENT]

    title = 'Rider Receivables Report - {}'.format(end_date)
    file_name = '{}.csv'.format(title)
    zip_file = create_csv(file_name, riders_data, header)
    attachments = [{'name': file_name + '.zip', 'content': zip_file.getvalue()}]
    import csv



    with open('countries.csv', 'w', encoding='UTF8') as f:
        writer = csv.writer(f)

        # write the header
        writer.writerow(header)

        # write the data
        writer.writerow(riders_data)
rider_receivables("2019-10-10", "2020-10-10")

