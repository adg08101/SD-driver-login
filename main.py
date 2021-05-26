import pymysql


def main(p_host, p_user, p_password, p_db, p_port):
    connection = pymysql.connect(host=p_host,
                                 user=p_user,
                                 password=p_password,
                                 database=p_db,
                                 port=int(p_port),
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection.cursor() as cursor:
        sql = "SELECT org_name, sd_db_source FROM smartconnect.orgs ORDER BY org_name ASC;"
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()

    for res in range(len(result)):
        print('NUM:', res, '---- ORG:', result[res]['org_name'])

    org_num = int(input("SELECT ORG: "))
    org_db = result[org_num]['sd_db_source']

    print("Selected Org =", result[org_num]['org_name'])

    connection.close()

    new_connection = pymysql.connect(host=p_host,
                                     user=p_user,
                                     password=p_password,
                                     database=org_db,
                                     port=int(p_port),
                                     cursorclass=pymysql.cursors.DictCursor)

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
                                                                                   "TRUCK_TRAILER_ID =\'%s\' " % int(
                trailer_id)
            cursor.execute(query)
            trailer = cursor.fetchone()
    else:
        trailer = 'null'

    if trailer:
        print("Selected Trailer =", trailer['TRUCK_TRAILER_NAME'])
    else:
        trailer_id = 'null'

    with new_connection.cursor() as cursor:
        query = "UPDATE " + org_db + ".driver_shift SET END_TIME = now() WHERE DRIVER_ID = \'%s\' AND " \
                                     "END_TIME IS NULL " % driver_id
        cursor.execute(query)
        query = "UPDATE " + org_db + ".driver_shift SET END_TIME = now() WHERE DRIVER_ID = \'%s\' AND " \
                                     "END_TIME IS NULL " % driver_id
        cursor.execute(query)

        query = f"INSERT INTO " + org_db + ".driver_shift (driver_id, truck_id, TRAILER_ID, shift_date, start_time, " \
                                           "DRIVER_SHIFT_STATUS_ID) VALUES ({ driver_id }, { truck_id }, { trailer_id " \
                                           "}, date(now()), time(DATE_ADD(now(), INTERVAL -4.5 HOUR)), 1) "

        print(query)

        cursor.execute(query)


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

    if password == '':
        password = 'gsi*2019'

    main(host,
         user,
         password,
         db,
         port)