import pymysql

HOST = ''
USER = ''
PASSWORD = ''
DB = ''
PORT = 0
CONNECTION = False
NEW_CONNECTION = False

datetime_format = "'%h:%i:%s %p')"


def main_proc(p_host=HOST, p_user=USER, p_password=PASSWORD, p_db=DB, p_port=PORT, p_connection=CONNECTION):
    print("Connecting...")
    if not p_connection:
        connection = pymysql.connect(host=p_host,
                                     user=p_user,
                                     password=p_password,
                                     database=p_db,
                                     port=int(p_port),
                                     cursorclass=pymysql.cursors.DictCursor)
        global HOST, USER, PASSWORD, DB, PORT, CONNECTION, NEW_CONNECTION
        HOST = p_host
        USER = p_user
        PASSWORD = p_password
        DB = p_db
        PORT = p_port
        CONNECTION = connection
    else:
        connection = CONNECTION

    print("Done... please wait for data retrieval...")

    with connection.cursor() as cursor:
        sql = f"SELECT org_name, sd_db_source FROM {p_db}.orgs ORDER BY org_name ASC;"
        cursor.execute(sql)
        organizations = cursor.fetchall()

        organizations_to_delete = list()

        for organization in range(len(organizations)):
            sql = (
                f"SELECT r.ROUTE_ID FROM {organizations[organization]['sd_db_source']}"
                f".route AS r WHERE r.ROUTE_DATE = date(now()) AND r.ROUTE_ID IS NOT NULL "
            )

            try:
                cursor.execute(sql)
                route = cursor.fetchall()
                if not route:
                    organizations_to_delete.append(organizations[organization]['org_name'])
            except pymysql.err.ProgrammingError:
                organizations_to_delete.append(organizations[organization]['org_name'])

        cursor.close()

    for organization in range(len(organizations)):
        if organizations[organization]['org_name'] in organizations_to_delete:
            organizations[organization]['show'] = 'false'
        else:
            organizations[organization]['show'] = 'true'

    organizations_temp = list()

    for organization in range(len(organizations)):
        if organizations[organization]['show'] == 'true':
            organizations_temp.append(organizations[organization])

    if len(organizations_temp) < 1:
        print("No routes available for today, create routes and then come back.")
        input()
        exit(0)

    print("-" * 50)

    for organization in range(len(organizations_temp)):
        print('NUM:', organization, '---- ORG:', organizations_temp[organization]['org_name'])
        print("-" * 50)

    org_num = get_input("Select organization", 0, len(organizations_temp) - 1)

    org_db = organizations_temp[org_num]['sd_db_source']

    print("Selected Org =", organizations_temp[org_num]['org_name'])

    if not p_connection:
        new_connection = pymysql.connect(host=p_host,
                                         user=p_user,
                                         password=p_password,
                                         database=org_db,
                                         port=int(p_port),
                                         cursorclass=pymysql.cursors.DictCursor)
        NEW_CONNECTION = new_connection

    fast_mode(connection, NEW_CONNECTION, org_db)


def get_input(text, min_value, max_value):
    while True:
        value = input(text + ": ")

        try:
            if int(value) < min_value or int(value) > max_value or not value.isdigit():
                raise ValueError("value_error")
            else:
                return int(value)
        except ValueError:
            print("Input value error")


def fast_mode(connection, new_connection, org_db):
    with new_connection.cursor() as cursor:
        query = (
            f"SELECT r.ROUTE_ID, b.BRANCH_NAME, ds.shift_id, r.DRIVER_ID, d.FIRST_NAME, d.LAST_NAME, r.TRUCK_ID, "
            f"t.TRUCK_NAME, "
            f"r.TRAILER_ID, tt.TRUCK_TRAILER_NAME FROM {org_db}.route AS r "
            f"INNER JOIN {org_db}.driver AS d ON d.DRIVER_ID = r.DRIVER_ID INNER JOIN {org_db}"
            f".truck AS t ON t.TRUCK_ID = r.TRUCK_ID "
            f"INNER JOIN {org_db}.branch AS b ON b.BRANCH_ID = r.BRANCH_ID "
            f"LEFT JOIN {org_db}.truck_trailer AS tt ON tt.TRUCK_TRAILER_ID = r.TRAILER_ID "
            f"LEFT JOIN {org_db}.driver_shift AS ds ON ds.DRIVER_ID = d.DRIVER_ID AND "
            f"ds.TRUCK_ID = t.TRUCK_ID AND ds.shift_date = date(now()) AND ds.end_time IS NULL "
            f"WHERE r.ROUTE_DATE = date(now()) "
        )

        cursor.execute(query)
        routes = cursor.fetchall()

    print("-" * 110)

    for route in range(len(routes)):
        print('NUM:', route, '---- Driver:', routes[route]['FIRST_NAME'], routes[route]['LAST_NAME'],
              'Branch:', routes[route]['BRANCH_NAME'], 'Status:', '**Online**' if routes[route]['shift_id'] else
              'Offline', 'Truck:', routes[route]['TRUCK_NAME'], 'Trailer:', routes[route]['TRUCK_TRAILER_NAME'])
        print("-" * 110)

    driver = get_input("Select Driver to Login/Logout", 0, len(routes) - 1)

    proc(connection, new_connection, org_db, routes[driver]['DRIVER_ID'], routes[driver]['TRUCK_ID'],
         routes[driver]['TRAILER_ID'])


def proc(connection, new_connection, org_db, driver_id, truck_id, trailer_id):
    with new_connection.cursor() as cursor:
        query = (
            f"SELECT shift_id FROM {org_db}.driver_shift WHERE DRIVER_ID = {driver_id} AND END_TIME IS "
            f"NULL AND shift_date = date(now()) "
        )

        cursor.execute(query)
        shift = cursor.fetchone()

    if shift:
        orders_proc(shift, connection, new_connection, driver_id, org_db, truck_id)
        logout(new_connection, org_db, driver_id, truck_id)
    else:
        login(connection, new_connection, org_db, driver_id, truck_id, trailer_id)

    repeat_proc(connection, new_connection)


def repeat_proc(connection, new_connection):
    repeat = get_input("Repeat process? (0) No / (1) Yes", 0, 1)
    if repeat == 1:
        main_proc(p_connection=True)
    else:
        connection.close()
        new_connection.close()
        exit(0)


def end_proc(new_connection, org_db, driver_id):
    with new_connection.cursor() as cursor:
        query = (
            f"UPDATE {org_db}.driver_shift SET END_DATE = date(now()) "
            f"WHERE DRIVER_ID = {driver_id} AND END_TIME IS NULL "
        )
        cursor.execute(query)
        new_connection.commit()

        query = (
            f"UPDATE {org_db}.driver_shift SET END_TIME = "
            f"TIME_FORMAT(time(DATE_ADD(now(), INTERVAL -4 HOUR)), {datetime_format} WHERE DRIVER_ID = {driver_id} AND "
            f"END_TIME IS NULL "
        )
        cursor.execute(query)
        new_connection.commit()


def orders_proc(shift, connection, new_connection, driver_id, org_db, truck_id):
    option = get_input("Driver Logged In, Log out (0) or Work with orders? (1)", 0, 1)

    if option == 1:
        with new_connection.cursor() as cursor:
            query = (
                f"SELECT MAX(route_id) AS ROUTE_ID FROM {org_db}.route WHERE "
                f"DRIVER_ID = {driver_id} AND TRUCK_ID = {truck_id}"
            )

            cursor.execute(query)
            route = cursor.fetchone()

            query = (
                f"SELECT o.*, os.ORDER_STATUS AS ORDER_STATUS FROM {org_db}.orders AS o "
                f"INNER JOIN {org_db}.order_status AS os ON o.ORD_STATUS_ID = os.ORD_STATUS_ID "
                f"WHERE o.route_id = {route['ROUTE_ID']} ORDER BY o.ROUTE_SEQ ASC"
            )

            cursor.execute(query)
            orders = cursor.fetchall()

            print("-" * 110)

            for order in range(len(orders)):
                print('NUM:', order, 'Order ID:', orders[order]['ORDER_ID'], 'Account:',
                      orders[order]['ACCOUNT_ID'], 'Status:', orders[order]['ORDER_STATUS'],
                      'Quantity:', orders[order]['ORD_QTY'],
                      'Sequence:', orders[order]['ROUTE_SEQ'], 'Date:', orders[order]['CREATED_DATE'], '\n',
                      'Urgency:', orders[order]['URGENCY_FACTOR'], 'Del date:', orders[order]['DELIVERY_DATE'],
                      'Del type:', orders[order]['DELIVERY_TYPE'], 'Is promised:', orders[order]['IS_PROMISED'])
                print("-" * 110)

            option = get_input("Choose option (0) Deliver all orders (1) Re-Route all orders", 0, 1)

            if option == 0:
                query = (
                    f"UPDATE {org_db}.orders AS o "
                    f"SET ORD_STATUS_ID = 4 "
                    f"WHERE o.route_id = {route['ROUTE_ID']} "
                )

                cursor.execute(query)
                new_connection.commit()

                query = (
                    f"UPDATE {org_db}.orders AS o "
                    f"SET DELIVERY_DATE = DATE_ADD(now(), INTERVAL -4 HOUR) "
                    f"WHERE o.route_id = {route['ROUTE_ID']} "
                )

                cursor.execute(query)
                new_connection.commit()

                query = (
                    f"UPDATE {org_db}.orders AS o "
                    f"SET DELIVERY_TYPE = 1 "
                    f"WHERE o.route_id = {route['ROUTE_ID']} "
                )

                cursor.execute(query)
                new_connection.commit()

                for order in range(len(orders)):
                    query = (
                        f"INSERT INTO {org_db}.delivery"
                        f"(ORDER_ID, PAYMENT_MODE, DEL_DATE, QTY, PAYMENT_RCVD, PAYMENT_AMOUNT, "
                        f"LONGITUDE, LATITUDE, POSTED, DEL_COMMENTS, DEL_TOTAL_AMT, DEL_BILLING_STATUS, "
                        f"DEL_TYPE_ID, DEL_ACT_UPD_NOTES, DEL_FLAG, DEL_TANK_PARTIAL_PERC, DEL_TANK_START_PARTIAL, "
                        f"DEL_LOCATION_DIFF, DEL_TANKSIZE_ALERT, LEAKTEST_REQUIRED_FLAG, LEAKTEST_SUCCESS_FLAG, RED_TAG_NUM, "
                        f"NO_ONE_HOME_NUM, LEAKTEST_CUSTOMER_HOME, NO_DELIV_REASON, MANUAL_FLAG, MANUAL_ENTRY_TIME, "
                        f"DEL_DURATION, PAYMENT_INFO, DRIVER_ID, SHIFT_ID, TOTALIZER_START, TOTALIZER_END, METER_SALE_NUMBER,"
                        f"PRICE_OVERRIDE, SEQUENCE_NUMBER, CONTAINER_ID, CONTAINER_TYPE_ID, DRIVER_OVERRIDE_PRODUCT_ID, "
                        f"CREATED_AT, UPDATED_AT, DELIVERY_HOST_ID, PRODUCT_ID) "
                        f"VALUES "
                        f"({orders[order]['ORDER_ID']}, 0, DATE_ADD(now(), INTERVAL - 4 HOUR), {orders[order]['ORD_QTY']}, 0, 0, "
                        f"0, 0, 1, null, {orders[order]['ORD_QTY']}, 0, "
                        f"1 , null, 1, 10, 10, "
                        f"0, 0, 0, 0, 0, "
                        f"0, 0, null, 0, null, "
                        f"0, null, {driver_id}, {shift['shift_id']}, 0, 0, null, "
                        f"0, 0, null, null, null, "
                        f"DATE_ADD(now(), INTERVAL - 4 HOUR), DATE_ADD(now(), INTERVAL - 4 HOUR), null, 7) "
                    )

                    cursor.execute(query)
                    new_connection.commit()

                    query = (
                        f"UPDATE {org_db}.driver_shift "
                        f"SET end_totalizer = end_totalizer + {orders[order]['ORD_QTY']} "
                        f"WHERE shift_id = {shift['shift_id']} "
                    )

                    cursor.execute(query)
                    new_connection.commit()

                    query = (
                        f"UPDATE {org_db}.driver_shift "
                        f"SET totalizer_total = totalizer_total + {orders[order]['ORD_QTY']} "
                        f"WHERE shift_id = {shift['shift_id']} "
                    )

                    cursor.execute(query)
                    new_connection.commit()
            else:
                query = (
                    f"UPDATE {org_db}.orders AS o "
                    f"SET ORD_STATUS_ID = 3 "
                    f"WHERE o.route_id = {route['ROUTE_ID']} "
                )

                cursor.execute(query)
                new_connection.commit()

                query = (
                    f"UPDATE {org_db}.orders AS o "
                    f"SET DELIVERY_DATE = null "
                    f"WHERE o.route_id = {route['ROUTE_ID']} "
                )

                cursor.execute(query)
                new_connection.commit()

                query = (
                    f"UPDATE {org_db}.orders AS o "
                    f"SET DELIVERY_TYPE = null "
                    f"WHERE o.route_id = {route['ROUTE_ID']} "
                )

                cursor.execute(query)
                new_connection.commit()

                query = f"DELETE FROM {org_db}.delivery WHERE SHIFT_ID = {shift['shift_id']} "

                cursor.execute(query)
                new_connection.commit()

            repeat_proc(connection, new_connection)
    else:
        pass


def login(connection, new_connection, org_db, driver_id, truck_id, trailer_id):
    with new_connection.cursor() as cursor:
        query = (
            f"SELECT MAX(shift_id) AS SHIFT_ID FROM {org_db}.driver_shift WHERE DRIVER_ID = {driver_id} "
            f"AND END_TIME IS NOT NULL AND shift_date = date(now()) "
        )

        cursor.execute(query)
        res = cursor.fetchone()

        option = None

        if res['SHIFT_ID']:
            option = get_input("Reopen today's Shift (0) or Open a new one (1)?", 0, 1)

        if option == 0:
            query = (
                f"UPDATE " + org_db + ".driver_shift "
                f"SET END_DATE = null "
                f"WHERE shift_id = {res['SHIFT_ID']}"
            )

            cursor.execute(query)
            new_connection.commit()

            query = (
                f"UPDATE " + org_db + ".driver_shift "
                f"SET END_TIME = null "
                f"WHERE shift_id = {res['SHIFT_ID']}"
            )

            cursor.execute(query)
            new_connection.commit()

            query = (
                f"UPDATE {org_db}.driver_shift "
                f"SET end_totalizer = 0 "
                f"WHERE shift_id = {res['SHIFT_ID']} "
            )

            cursor.execute(query)
            new_connection.commit()

            query = (
                f"UPDATE {org_db}.driver_shift "
                f"SET totalizer_total = 0 "
                f"WHERE shift_id = {res['SHIFT_ID']} "
            )

            cursor.execute(query)
            new_connection.commit()
        else:
            end_proc(new_connection, org_db, driver_id)

            if trailer_id is None:
                trailer_id = 'null'

            query = (
                f"INSERT INTO " + org_db + ".driver_shift (driver_id, truck_id, TRAILER_ID, shift_date, "
                f"start_time, DRIVER_SHIFT_STATUS_ID) VALUES ({driver_id}, {truck_id}, {trailer_id}, date(now()), "
                f"TIME_FORMAT(time(DATE_ADD(now(), INTERVAL -4.1 HOUR)), {datetime_format}, 1)"
            )

            cursor.execute(query)
            new_connection.commit()

        query = f"UPDATE {org_db}.route SET ROUTE_STATUS_ID = 3 WHERE DRIVER_ID = {driver_id} AND TRUCK_ID = {truck_id}"

        cursor.execute(query)
        new_connection.commit()

        print("Driver Logged In successfully")

        orders_proc(res['SHIFT_ID'], connection, new_connection, driver_id, org_db, truck_id)


def logout(new_connection, org_db, driver_id, truck_id):
    with new_connection.cursor() as cursor:
        shift_status = get_input("Set Shift status as (1) Active (2) Logout (3) End of day", 1, 3)
        query = (
            f"UPDATE {org_db}.driver_shift SET DRIVER_SHIFT_STATUS_ID = {shift_status} WHERE DRIVER_ID = {driver_id} "
            f"AND END_TIME IS NULL "
        )
        cursor.execute(query)
        new_connection.commit()

        end_proc(new_connection, org_db, driver_id)

        route_status = get_input("Set Route status as (0) Finished (1) Created", 0, 1)

        if route_status == 0:
            route_status = 4

        query = (
            f"UPDATE {org_db}.route SET ROUTE_STATUS_ID = {route_status} WHERE DRIVER_ID = {driver_id} AND "
            f"TRUCK_ID = {truck_id} "
        )

        cursor.execute(query)
        new_connection.commit()

        print("Driver Logged Out successfully")


if __name__ == '__main__':
    while True:
        host = input("HOST ('smartdrops.gsoftinnovation.net'): ")
        db = input("DB ('smartconnect'): ")
        port = input("PORT (3306):")
        user = input("USER ('root'):")
        password = input("PASS: ")

        if host == '':
            host = 'smartdrops.gsoftinnovation.net'

        if db == '':
            db = 'smartconnect'

        if port == '':
            port = 3306

        if user == '':
            user = 'root'

        try:
            main_proc(host,
                      user,
                      password,
                      db,
                      port,
                      p_connection=False)
            break
        except (pymysql.err.OperationalError, ValueError) as err:
            print("Connection error, could not connect to DB")
            print(err)
