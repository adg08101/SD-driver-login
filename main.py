import pymysql


def main(p_host, p_user, p_password, p_db, p_port):
    connection = pymysql.connect(host=p_host,
                                 user=p_user,
                                 password=p_password,
                                 database=p_db,
                                 port=int(p_port),
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection.cursor() as cursor:
        sql = "SELECT org_name, sd_db_source FROM " + p_db + ".orgs ORDER BY org_name ASC;"
        cursor.execute(sql)
        organizations = cursor.fetchall()

        organizations_to_delete = list()

        for organization in range(len(organizations)):
            sql = "SELECT r.ROUTE_ID FROM " + organizations[organization]['sd_db_source'] +\
                  ".route AS r WHERE r.ROUTE_DATE = date(now()) AND r.ROUTE_ID IS NOT NULL "

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

    print("-----------------------------")

    for organization in range(len(organizations_temp)):
        if organizations_temp[organization]['show'] == 'true':
            print('NUM:', organization, '---- ORG:', organizations_temp[organization]['org_name'])
            print("-----------------------------")

    org_num = int(input("SELECT ORG: "))
    org_db = organizations_temp[org_num]['sd_db_source']

    print("Selected Org =", organizations_temp[org_num]['org_name'])

    connection.close()

    new_connection = pymysql.connect(host=p_host,
                                     user=p_user,
                                     password=p_password,
                                     database=org_db,
                                     port=int(p_port),
                                     cursorclass=pymysql.cursors.DictCursor)

    fast_mode(new_connection, org_db)

    """choice = input("Select mode (1) Fast Mode or (2) Normal Mode: ")

    if choice == '1':
        fast_mode(new_connection, org_db)
    else:
        normal_mode(new_connection, org_db)"""


def fast_mode(new_connection, org_db):
    with new_connection.cursor() as cursor:
        query = "SELECT r.ROUTE_ID, ds.shift_id, r.DRIVER_ID, d.FIRST_NAME, d.LAST_NAME, r.TRUCK_ID, t.TRUCK_NAME, " \
                "r.TRAILER_ID, tt.TRUCK_TRAILER_NAME FROM " + org_db + ".route AS r " \
                "INNER JOIN " + org_db + ".driver AS d ON d.DRIVER_ID = r.DRIVER_ID INNER JOIN " + org_db + \
                ".truck AS t ON t.TRUCK_ID = r.TRUCK_ID " \
                "LEFT JOIN " + org_db + ".truck_trailer AS tt ON tt.TRUCK_TRAILER_ID = r.TRAILER_ID " \
                "LEFT JOIN " + org_db + ".driver_shift AS ds ON ds.DRIVER_ID = d.DRIVER_ID AND " \
                "ds.TRUCK_ID = t.TRUCK_ID AND ds.shift_date = date(now()) AND ds.end_time IS NULL " \
                "WHERE r.ROUTE_DATE = date(now()) "

        cursor.execute(query)
        routes = cursor.fetchall()

    print("------------------------------------------------------------------------------------------------------")

    for route in range(len(routes)):
        print('NUM:', route, '---- Driver:', routes[route]['FIRST_NAME'], routes[route]['LAST_NAME'],
              'Status:', '**Online**' if routes[route]['shift_id'] else 'Offline', 'Truck:',
              routes[route]['TRUCK_NAME'], 'Trailer:', routes[route]['TRUCK_TRAILER_NAME'])
        print("------------------------------------------------------------------------------------------------------")

    driver = int(input("Select Driver to Login/Logout: "))

    proc(new_connection, org_db, routes[driver]['DRIVER_ID'], routes[driver]['TRUCK_ID'], routes[driver]['TRAILER_ID'])


def normal_mode(new_connection, org_db):
    driver_id = int(input("Driver ID: "))

    with new_connection.cursor() as cursor:
        query = "SELECT DRIVER_ID, FIRST_NAME, LAST_NAME FROM " + org_db + ".driver WHERE DRIVER_ID " \
                                                                           "=\'%s\' " % driver_id
        cursor.execute(query)
        driver = cursor.fetchone()

    print("Selected Driver =", driver['FIRST_NAME'], driver['LAST_NAME'])

    truck_id = int(input("Truck ID: "))

    with new_connection.cursor() as cursor:
        query = "SELECT TRUCK_ID, TRUCK_NAME FROM " + org_db + ".truck WHERE TRUCK_ID " \
                                                               "=\'%s\' " % truck_id
        cursor.execute(query)
        truck = cursor.fetchone()

    print("Selected Truck =", truck['TRUCK_NAME'])

    trailer_id = input("Trailer ID (None): ")

    if trailer_id != '':
        with new_connection.cursor() as cursor:
            query = "SELECT TRUCK_TRAILER_ID, TRUCK_TRAILER_NAME FROM " + org_db + ".truck_trailer WHERE " \
                                                                                   "TRUCK_TRAILER_ID =\'%s\' " \
                    % int(trailer_id)

            cursor.execute(query)
            trailer = cursor.fetchone()
    else:
        trailer = 'null'

    if trailer != 'null':
        print("Selected Trailer =", trailer['TRUCK_TRAILER_NAME'])
    else:
        print("Selected Trailer = None")
        trailer_id = 'null'

    proc(new_connection, org_db, driver_id, truck_id, trailer_id)


def proc(new_connection, org_db, driver_id, truck_id, trailer_id):
    # choice = input("Select option (1) Login or (2) Logout: ")

    with new_connection.cursor() as cursor:
        query = "SELECT shift_id FROM " + org_db + ".driver_shift WHERE DRIVER_ID = \'%s\' AND END_TIME IS " \
                                                   "NULL AND shift_date = date(now()) " % driver_id

        cursor.execute(query)
        res = cursor.fetchone()

    if res:
        logout(new_connection, org_db, driver_id, truck_id)
    else:
        login(new_connection, org_db, driver_id, truck_id, trailer_id)

    input()


def login(new_connection, org_db, driver_id, truck_id, trailer_id):
    with new_connection.cursor() as cursor:
        query = "UPDATE " + org_db + ".driver_shift SET END_DATE = now() WHERE DRIVER_ID = \'%s\' AND " \
                                     "END_TIME IS NULL " % driver_id
        cursor.execute(query)
        new_connection.commit()

        query = "UPDATE " + org_db + ".driver_shift SET END_TIME = now() WHERE DRIVER_ID = \'%s\' AND " \
                                     "END_TIME IS NULL " % driver_id
        cursor.execute(query)
        new_connection.commit()

        query = "UPDATE " + org_db + ".route SET ROUTE_STATUS_ID = 4 WHERE DRIVER_ID = \'%s\' AND TRUCK_ID " \
                                     "= \'%s\' " % (driver_id, truck_id)

        cursor.execute(query)
        new_connection.commit()

        if trailer_id is None:
            trailer_id = 'null'

        query = "INSERT INTO " + org_db + ".driver_shift (driver_id, truck_id, TRAILER_ID, shift_date, " \
                                          "start_time, DRIVER_SHIFT_STATUS_ID) VALUES (%s, %s, %s, date(now()), " \
                                          "TIME_FORMAT(time(DATE_ADD(" \
                                          "now(), INTERVAL -4.5 HOUR)), " % (driver_id, truck_id, trailer_id)

        query += "'%h:%i:%s %p'), 1) "

        cursor.execute(query)
        new_connection.commit()

        query = "UPDATE " + org_db + ".route SET ROUTE_STATUS_ID = 3 WHERE DRIVER_ID = \'%s\' AND TRUCK_ID = \'%s\' " \
                % (driver_id, truck_id)

        cursor.execute(query)
        new_connection.commit()

        new_connection.close()

        print("Driver Logged In successfully")


def logout(new_connection, org_db, driver_id, truck_id):
    with new_connection.cursor() as cursor:
        query = "UPDATE " + org_db + ".driver_shift SET END_DATE = now() WHERE DRIVER_ID = \'%s\' AND " \
                                     "END_TIME IS NULL " % driver_id
        cursor.execute(query)
        new_connection.commit()

        query = "UPDATE " + org_db + ".driver_shift SET DRIVER_SHIFT_STATUS_ID = 2 WHERE DRIVER_ID = \'%s\' AND " \
                                     "END_TIME IS NULL " % driver_id
        cursor.execute(query)
        new_connection.commit()

        query = "UPDATE " + org_db + ".driver_shift SET END_TIME = now() WHERE DRIVER_ID = \'%s\' AND " \
                                     "END_TIME IS NULL " % driver_id
        cursor.execute(query)
        new_connection.commit()

        query = "UPDATE " + org_db + ".route SET ROUTE_STATUS_ID = 4 WHERE DRIVER_ID = \'%s\' AND TRUCK_ID = \'%s\' " \
                % (driver_id, truck_id)

        cursor.execute(query)
        new_connection.commit()

        new_connection.close()

        print("Driver Logged Out successfully")


if __name__ == '__main__':
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

    main(host,
         user,
         password,
         db,
         port)
