from utils import *


def get_data():
    get_data_query = ("""SELECT r.id, r.cash_in_hand,CONCAT(au.first_name,'',au.last_name) as ridername ,c.name,r.mobile_number ,r.nic FROM rider r INNER JOIN city c ON (r.city_id = c.id) INNER JOIN auth_user au ON 
                    (r.user_id = au.id) WHERE (r.cash_in_hand > 0.0 AND r.city_id IS NOT NULL AND au.is_active = True) limit 5 """)

    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(get_data_query)
    get_query = cursor.fetchall()
    return get_query


def last_settlement_query(end_date,rider):
    last_settlement_sql = ("""SELECT rc.id ,rc.created_at FROM rider_cash rc 
                            WHERE (DATE(CONVERT_TZ(rc.created_at , 'UTC', 'UTC')) <= '{}' AND rc.log_type 
                            IN (4, 5) AND rc.rider_id = '{}') ORDER BY rc.id DESC limit 1""".format(end_date, rider))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(last_settlement_sql)
    settlement_query = cursor.fetchall()
    return settlement_query


def logistics_configuration_instance():

    LogisticsConfiguration_instance_sql = ("""select lc.enforce_fuel_deduction_date from logistics_configuration lc limit 1""")
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(LogisticsConfiguration_instance_sql)
    instance = cursor.fetchall()
    return instance[0]


def rc_sum_query(end_date, rider):
    rc_sum_query_sql = ("""SELECT sum(case when rc.trans_type = 'c' then amount  end) as trans_type_c,
                        sum(case when rc.trans_type = 'd' then amount  end) as trans_type_d
                        FROM rider_cash rc WHERE (DATE(CONVERT_TZ(rc.created_at , 'UTC', 'UTC')) > '{}'
                        AND rc.log_type IN (1, 2, 3) AND rc.rider_id = '{}')""".format(end_date, rider))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(rc_sum_query_sql)
    rc_sum_sql = cursor.fetchall()
    return rc_sum_sql


def get_equipment_cost(rider, start_time, end_time):
    get_equipment_cost_sql = ("""SELECT sum(rel.cost) as amount FROM rider_equipment_log rel WHERE (rel.created_at 
                                BETWEEN '{}' AND '{}' AND rel.rider_id = '{}')""".format(start_time, end_time, rider))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(get_equipment_cost_sql)
    equipment_cost = cursor.fetchall()
    return equipment_cost[0][0] or 0


def get_rider_pickup_distances(rider, start_time, end_time, log_type):
    pick_up_distance_sql = ("""select  sum(od.pickup_distance) from `order`o right join rider_earnings re 
                    on o.id=re.order_id
                    inner join order_distance od 
                    on od.order_id = o.id  
                    WHERE (re.created_at BETWEEN '{}' AND 
                    '{}') AND 
                    re.log_type ='{}' and
                    re.rider_id='{}'
                    """.format(start_time, end_time, log_type, rider))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(pick_up_distance_sql)
    pick_up_distance = cursor.fetchall()
    if pick_up_distance[0][0]:
        return pick_up_distance[0][0]
    return 0


def get_rider_drop_off_distances(rider, start_time, end_time, log_type):

    drop_off_distance_sql = (""" select sum(od.delivered_distance) from `order`o right join rider_earnings re 
                                on o.id=re.order_id
                                inner join order_distance od 
                                on od.order_id = o.id  
                                WHERE (re.created_at BETWEEN '{}' AND 
                                '{}') AND 
                                re.log_type ='{}' and
                                re.rider_id='{}'
                                 """.format(start_time, end_time, log_type, rider))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(drop_off_distance_sql)
    drop_off_distance = cursor.fetchall()
    if drop_off_distance[0][0]:
        return drop_off_distance[0][0]
    return 0


def get_rider_earnings(rider, start_time, end_time):
    rider_earnings_sql = ("""SELECT  
                        SUM(CASE when re.log_type="PB" then amount  END) as pick_up_distance_bonus ,
                        SUM(CASE when re.log_type="PP" then amount  END) as pick_up_pay ,
                        SUM(CASE when re.log_type="DDP" then amount  END) as drop_off_distance_pay,
                        SUM(CASE when re.log_type="DP" then amount  END) as drop_off_pay,
                        SUM(CASE when re.log_type="DCP" then amount  END) as delivery_charges_based_pay,
                        SUM(CASE when re.log_type="FP" then amount  END) as per_order_pay,
                        SUM(CASE when re.log_type="SBP" then amount  END) as slab_based_pay,
                        SUM(CASE when re.log_type="TP" then amount  END) as tip_pay,

                        COUNT(case when re.log_type="PP" then order_id  END) total_pick_ups,
                        COUNT(case when re.log_type="DP" then order_id  END) total_drop_offs,
                        COUNT(case when re.log_type="FP" then order_id  END) total_per_order_pays,
                        COUNT(case when re.log_type="SBP" then order_id  END) total_slab_based_pays,
                        SUM(CASE when re.log_type="LNB" then amount  END) as total_late_night_bonus_pay
                        from rider_earnings re  where re.created_at BETWEEN '{}'
                        AND '{}'
                        AND re.rider_id ='{}' 
                        """.format(start_time, end_time, rider))
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(rider_earnings_sql)
    drop_off_distance = cursor.fetchall()
    return drop_off_distance