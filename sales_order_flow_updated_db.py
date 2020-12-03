import csv
import pywinauto.findwindows as find
from pywinauto import Application
import requests
import time
import pywinauto.keyboard as keyboard
import os
import shutil
import os.path as path
import subprocess
import csv_utils
import rpa_lib.args_utils as args
import mysql
from sending_mail import *
import datetime
import mysql.connector
import pyautogui
from rpa_lib import Logger
from pywinauto.timings import Timings

base_url = "https://order.retailio.in"
update_url = base_url + "/api/integrations/distributor-integration/updateOrderStatus"
orders_url = (
    base_url
    + "/api/integrations/distributor-integration/fetchOrdersForSynchroniser?limit=1&offset=0&status=Pending"
)

app_path = args.get_config("pharmassist_path")
exe_name = app_path[app_path.rindex("\\") + 1:]
distributor_id = args.get_config("distributor_id")
distributor_name = args.get_config("distributor_name")
sms_alert_numbers = args.get_config("sms_number")
username = args.get_config("login_username")
password = args.get_config("login_password")
alert_enabled = args.get_config("alert_enabled")

interim_title = "SHIVHARI PHARMACEUTICALS"
main_title = "SHIVHARI PHARMACEUTICALS.*Sales Order"

mail = MailSend()
mail.distributor_name = distributor_name
logger = Logger().get_logger()
Timings.fast()

db_config = {
    "host": "haproxy.ahwspl.net",
    "port": 51002,
    "user": "mubpamro",
    "password": "HipHodVidLoDrykOncayRet0",
    "database": "rpa",
}
headers = {
    "version": "1.0.0",
    "source": "api-client",
    "Content-Type": "application/json",
    "Authorization": "Bearer 5af2c2e0-8066-11ea-b12d-a7ce185fd2a2",
}


def create_directory_if_not_exists(*directories):
    for dir in directories:
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)


screeshots = os.path.join(os.getcwd(), "screenshots")
sales_order_base_folder = os.path.join(os.getcwd(), "sales-order")
master_folder = os.path.join(sales_order_base_folder, "master")
input_folder = os.path.join(sales_order_base_folder, "input")
output_folder = os.path.join(sales_order_base_folder, "output")
create_directory_if_not_exists(
    screeshots, master_folder, input_folder, output_folder)

separator = "-----------------------------------------------------------------------------------"

main_win_ref = None


def format_time():
    return datetime.datetime.now().strftime("%d-%m-%Y-%H-%M-%S")


def capture_screenshot(name):
    pyautogui.screenshot(os.path.join(
        screeshots, "{}_{}.png".format(name, format_time())))


def alert_by_sms(msg):
    if alert_enabled:
        url = "https://enterprise.smsgupshup.com/GatewayAPI/rest?method=SendMessage&send_to={}&msg={} {} at {}&msg_type=TEXT&userid=2000190762&auth_scheme=plain&password=bdm9MAYd7&v=1.1&format=text".format(
            sms_alert_numbers, distributor_name, msg, datetime.datetime.now()
        )

        with requests.Session() as req:
            req.get(url, verify=True)


def get_database_connection():
    def get_connection(retry_count):
        try:
            if retry_count > 0:
                return mysql.connector.connect(
                    host=db_config["host"],
                    port=db_config["port"],
                    user=db_config["user"],
                    password=db_config["password"],
                    database=db_config["database"],
                )
            else:
                return None
        except:
            retry_count = retry_count - 1

    return get_connection(3)


def database_update(
    unique_id,
    customer_code,
    so_number,
    order_value,
    total_item,
    order_time,
    order_status,
    order_json,
    order_status_req,
    json_response,
    start_time,
    end_time,
    item_count,
    retailer_name,
):
    mydb = None

    try:
        now = datetime.datetime.now()

        mydb = get_database_connection()
        mycursor = mydb.cursor()

        sql = """INSERT INTO rpa.sales_order (created_on, fk_process_id, fk_bot_id, order_source, source_order_id, order_date, cust_code, cust_name, order_json, dest_order_id, total_items, order_value, status, reason, start_time, end_time, is_active, updated_on) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        val = (
            now,
            "16",
            "12",
            "rio",
            unique_id,
            order_time,
            customer_code,
            retailer_name,
            json_response,
            so_number,
            "Input::"+str(item_count)+"|Erp::"+total_item,
            order_value,
            order_status,
            "N.A",
            start_time,
            end_time,
            "1",
            now,
        )

        mycursor.execute(sql, val)
        mydb.commit()
        logger.info("{} record(s) inserted".format(mycursor.rowcount))

    except:
        if alert_enabled and (mydb == None or not mydb.is_connected()):
            mail.sql_db_error()
            alert_by_sms(
                "Sales order RPA Python SQL Production Database not working")
    finally:
        if mydb != None and mydb.is_connected():
            mydb.close()


def updating_order(
    input_file,
    unique_id,
    customer_code,
    so_number,
    order_value,
    total_item,
    responsejson,
    retailio_order_time,
    json_post,
    json_response,
    start_time,
    end_time,
    item_count,
    retailer_name,
):
    logger.info(separator)
    filename = os.path.join(output_folder, distributor_name + "Output.csv")
    file_exists = os.path.isfile(filename)
    with open(filename, "a") as f:
        headers = [
            "Date",
            "UniqueID",
            "PartyCode",
            "ERPReferenceID",
            "OrderStatus",
            "Reason",
            "OrderValue",
            "TotalItems",
        ]
        writer = csv.DictWriter(f, lineterminator="\n", fieldnames=headers)

        if not file_exists:
            writer.writeheader()
        writer.writerows(
            [
                {
                    "Date": datetime.datetime.today(),
                    "UniqueID": unique_id,
                    "PartyCode": customer_code,
                    "ERPReferenceID": so_number,
                    "OrderStatus": "Processed",
                    "Reason": "N/A",
                    "OrderValue": order_value,
                    "TotalItems": total_item,
                }
            ]
        )
    f.close()
    order_status = "Processed"

    # Masterfile
    filename = os.path.join(master_folder, "Masterfile.csv")
    file_exists = os.path.isfile(filename)

    with open(filename, "a") as f:
        headers = [
            "Date",
            "UniqueID",
            "PartyCode",
            "ERPReferenceID",
            "OrderStatus",
            "Reason",
            "OrderValue",
            "TotalItems",
        ]
        writer = csv.DictWriter(f, lineterminator="\n", fieldnames=headers)

        if not file_exists:
            writer.writeheader()
        writer.writerows(
            [
                {
                    "Date": datetime.datetime.today(),
                    "UniqueID": unique_id,
                    "PartyCode": customer_code,
                    "ERPReferenceID": so_number,
                    "OrderStatus": "Processed",
                    "Reason": "N/A",
                    "OrderValue": order_value,
                    "TotalItems": total_item,
                }
            ]
        )
    f.close()

    file_name = input_file[input_file.replace("/", "\\").rindex("\\") + 1:]
    shutil.move(input_file, path.join(input_folder, file_name))

    database_update(
        unique_id,
        customer_code,
        so_number,
        order_value,
        total_item,
        retailio_order_time,
        order_status,
        responsejson,
        json_post,
        json_response,
        start_time,
        end_time,
        item_count,
        retailer_name,
    )
    logger.info(separator)


def database_update_failed(unique_id, customer_code, order_time, order_status_req, json_response, retailer_name):
    mydb = None
    try:
        now = datetime.datetime.now()

        mydb = get_database_connection()
        mycursor = mydb.cursor()

        sql = """INSERT INTO rpa.sales_order (created_on, fk_process_id, fk_bot_id, order_source, source_order_id, order_date, cust_code, cust_name, order_json, dest_order_id, total_items, order_value, status, reason, start_time, end_time, is_active, updated_on) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        val = (
            now,
            "16",
            "12",
            "rio",
            unique_id,
            order_time,
            customer_code,
            retailer_name,
            json_response,
            "N.A",
            "N.A",
            "N.A",
            "Failed",
            "Customer Not Found",
            None,
            None,
            "1",
            now,
        )
        mycursor.execute(sql, val)
        mydb.commit()
        logger.info("{} record(s) inserted".format(mycursor.rowcount))
    except:
        if alert_enabled and (mydb == None or not mydb.is_connected()):
            mail.sql_db_error()
            alert_by_sms("Sales order RPA Python ERP database error")
    finally:
        if mydb != None and mydb.is_connected():
            mydb.close()


def updating_failed(input_file, unique_id, customer_code, retailio_order_time, json, retailer_name):
    filename = os.path.join(output_folder, distributor_name + "Output.csv")
    file_exists = os.path.isfile(filename)
    with open(filename, "a") as f:
        headers = [
            "Date",
            "UniqueID",
            "PartyCode",
            "ERPReferenceID",
            "OrderStatus",
            "Reason",
            "OrderValue",
            "TotalItems",
        ]
        writer = csv.DictWriter(f, lineterminator="\n", fieldnames=headers)

        if not file_exists:
            writer.writeheader()
        writer.writerows(
            [
                {
                    "Date": datetime.datetime.now(),
                    "UniqueID": unique_id,
                    "PartyCode": customer_code,
                    "ERPReferenceID": "N.A",
                    "OrderStatus": "Failed",
                    "Reason": "Invalid Customer Code",
                    "OrderValue": "N.A",
                    "TotalItems": "N.A",
                }
            ]
        )

        # Masterfile

        filename = os.path.join(master_folder, "Masterfile.csv")
        file_exists = os.path.isfile(filename)
        with open(filename, "a") as f:
            headers = [
                "Date",
                "UniqueID",
                "PartyCode",
                "ERPReferenceID",
                "OrderStatus",
                "Reason",
                "OrderValue",
                "TotalItems",
            ]
            writer = csv.DictWriter(f, lineterminator="\n", fieldnames=headers)

            if not file_exists:
                writer.writeheader()
            writer.writerows(
                [
                    {
                        "Date": datetime.datetime.now(),
                        "UniqueID": unique_id,
                        "PartyCode": customer_code,
                        "ERPReferenceID": "N.A",
                        "OrderStatus": "Failed",
                        "Reason": "Invalid Customer Code",
                        "OrderValue": "N.A",
                        "TotalItems": "N.A",
                    }
                ]
            )
        f.close()

    file_name = input_file[input_file.replace("/", "\\").rindex("\\") + 1:]
    shutil.move(input_file, path.join(input_folder, file_name))

    database_update_failed(unique_id, customer_code, retailio_order_time, json, retailer_name)


def update_customer_not_found(dict):
    request_json = (
        '{"data": [{"uniqueId":'
        + dict["unique_id"]
        + ' ,"erpReferenceId": \
            "N.A" ,"orderStatus": "Failed"}]}'
    )
    response = None

    with requests.Session() as req:
        response = req.post(update_url, data=request_json,
                            headers=headers, verify=True)

    response_json = response.text
    logger.info("api update_customer_not_found: {}".format(response_json))
    # updating failed party code
    updating_failed(
        dict["order_file"],
        dict["unique_id"],
        dict["customer_code"],
        dict["order_time"],
        response_json,
        dict["retailer_name"]
    )
    # mail
    if alert_enabled:
        mail.customer_not_found()


def update_status_to_processed(dict):
    request_json = (
        '{"data": [{"uniqueId":'
        + dict["unique_id"]
        + ' ,"erpReferenceId":'
        + dict["so_number"]
        + ' ,"orderStatus": "Processed"}]}'
    )
    response = None

    with requests.Session() as req:
        response = req.post(update_url, data=request_json,
                            headers=headers, verify=True)

    logger.info("update response code: {}".format(response.status_code))
    update_json_body = response.text
    logger.info("update response body: {}".format(update_json_body))

    updating_order(
        dict["order_file"],
        dict["unique_id"],
        dict["customer_code"],
        dict["so_number"],
        dict["order_value"],
        dict["total_item"],
        update_json_body,
        dict["order_time"],
        request_json,
        dict["request_json"],
        dict["start_time"],
        dict["end_time"],
        dict["item_count"],
        dict["retailer_name"],
    )


def get_window(name, timeout=1):
    start = time.time()
    max = start + timeout

    while time.time() <= max:
        try:
            ids = find.find_windows(title_re=".*{}.*".format(name))
            if ids != None and len(ids) > 0:
                id = ids[0]
                logger.info(
                    "window with title '{}' is found in {} seconds".format(
                        name, round(time.time() - start, 2)
                    )
                )
                window = Application(backend="uia").connect(
                    handle=id).window(handle=id)
                window.set_focus()
                return window
        except:
            pass
    logger.error(
        "window with title '{}' is not found in {} seconds".format(
            name, round(time.time() - start, 2)
        )
    )
    return None


def is_window_exists(name, timeout=1):
    return None != get_window(name, timeout)


def click_if_present(name, timeout=1):
    if is_window_exists(name, timeout):
        keyboard.send_keys("~")


def kill_exec(name):
    subprocess.call("taskkill /f /im " + name)


def start_exec(exec, kill_existing=True):
    index = exec.rindex("\\")
    exec_name = exec[index + 1:]
    dir = exec[0:index]

    kill_exec(exec_name)
    subprocess.Popen(exec, cwd=dir, start_new_session=True)


def type_keys(keys, elm=None, pause=0):
    logger.debug(
        "entering keys '{}' into {}".format(keys, elm.window_text())
        if elm != None
        else "entering keys '{}'".format(keys)
    )
    #keys = keys.replace('(','{(}').replace(')','{)}')
    if elm != None:
        # elm.set_focus()
        # elm.click_input()
        elm.type_keys(keys, with_spaces=True, pause=pause)
    else:
        keyboard.send_keys(keys, with_spaces=True, pause=pause)


def wait_for_app_launch(timeout=30):
    start = time.time()
    max = start + timeout
    while time.time() <= max:
        windows = find.find_windows(title="")
        for win in windows:
            tmp = Application(backend="uia").connect(
                handle=win).windows(handle=win)[0]
            if len(tmp.descendants(control_type="Edit", title="c_logon")) > 0:
                tmp.set_focus()
                logger.debug(
                    "app launched in {} seconds".format(
                        round(time.time() - start), 2)
                )
                return tmp
    return None


def launch_app():
    start_exec(app_path)
    win = wait_for_app_launch()
    elms = win.descendants(control_type="Edit", title="c_logon")
    str = "{TAB 2}" + username + "{TAB}" + password + "~"
    type_keys(str, elms[0])
    if not is_window_exists("Warning!"):
        win = wait_for_app_launch(1)
        if win != None:
            elms = win.descendants(control_type="Edit")
            type_keys(str, elms[0])

    warn_window = get_window("Warning!", 5)
    if None != warn_window:
        logger.info(
            "waring message: {}".format(
                warn_window.descendants(control_type="Text")[0].window_text()
            )
        )
        type_keys("~")

    if is_window_exists("Version Mismatch!"):
        dlg = get_window("Version Mismatch!").descendants()[2].window_text()
        logger.info("version mismatch message: {}".format(dlg))
        version_number = dlg.split("Server")[1].split(" Build")[
            0].split(": ")[1]
        type_keys("~")
        time.sleep(1)
        type_keys("~")
        type_keys("~")
        time.sleep(1)
        type_keys("~")

        if alert_enabled:
            alert_by_sms("RPA Python ERP version mismatch")
            mail.version_mismatch(dlg)

    if is_window_exists(interim_title, 10):
        type_keys("^+o")


def init_app():
    try:
        if is_window_exists("Error"):
            kill_exec(exe_name)
            launch_app()
            return

        if is_window_exists(main_title):
            #type_keys("{VK_ESCAPE}")
            # time.sleep(1)
            type_keys("^e")
            if is_window_exists("Warning"):
                type_keys("y")

        if is_window_exists(interim_title, 5):
            type_keys("^+o")
            if is_window_exists(main_title, 5):
                type_keys("^n")
        else:
            launch_app()

    finally:
        time.sleep(1)


def txt(elm):
    try:
        return elm.get_value()
    except:
        try:
            return elm.iface_value.CurrentValue
        except:
            return None


def fetch(name, get_text=True, index=-1, window=None):
    start = time.time()
    main_win_title = "Sales Order"
    matches = []
    try:
        ids = find.find_window(title_re=".*{}".format(main_win_title))
        children = []
        if window == None:
            children = (
                Application(backend="uia")
                .connect(handle=id)
                .window(handle=id)
                .child_window(title="Workspace")
                .children()[0]
                .descendants(title=name)
            )
        else:
            children = window.descendants(title=name)
        if children != None and len(children) > 0:
            for child in children:
                matches.append(child)

        if len(matches) > 0:
            return txt(matches[-1]) if get_text else matches[-1]
        else:
            return None

    finally:
        logger.debug(
            "time taken to fetch '{}' => {}  seconds".format(
                name, round(time.time() - start, 2)
            )
        )


def write_to_csv(headers, csv_data, file):
    with open(file, "w", newline="\n") as csvfile:
        fieldnames = headers
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_data)
        logger.info("writing to '{}'".format(file))
    csvfile.close()


def fetch_orders():
    def get_order(retry_count):
        try:
            if retry_count > 0:
                with requests.Session() as req:
                    response = req.get(
                        orders_url, headers=headers, verify=True)
                    if response is not None and response != None:
                        return response
            else:
                return None
        except:
            if retry_count < 0:
                return None
            logger.warn("retrying to fetch orders")
            retry_count = retry_count - 1

    response = get_order(3)

    order_files = []

    if response == None or response.status_code != 200:
        if response == None:
            logger.error("unable to fetch orders")
        else:
            logger.error(
                "fetch orders failed with status code '{}'".format(
                    response.status_code)
            )
            # mail send
            if alert_enabled:
                mail.invalid_api()
        return order_files

    json = response.text

    logger.info("order response =>  {}".format(json))

    orders = response.json()["orders"]

    csv_headers = [
        "UniqueID",
        "DistributorRetailerCode",
        "DistributorItemCode",
        "OrderQuantity",
        "OrderTime",
        "RetailerName",
    ]

    order_files = []

    for order in orders:
        id = str(order["uniqueId"])
        order_date = order["orderDate"]
        retailer_code = order["distributorRetailerCode"]
        items = order["orderItems"]
        retailer_name = order["retailerName"]
        # Deleting existing csv file from input folder if any
        input = os.path.join(input_folder, id + ".csv")
        if os.path.exists(input):
            os.remove(input)
        else:
            logger.info("warn: input file '{}' to delete".format(input))

        csv_data = []
        if len(items) > 0:
            for item in items:
                item_code = item["distributorItemCode"]
                quantity = item["orderedQuantity"]
                csv_data.append(
                    {
                        "UniqueID": id,
                        "DistributorRetailerCode": retailer_code,
                        "DistributorItemCode": item_code,
                        "OrderQuantity": quantity,
                        "OrderTime": order_date,
                        "RetailerName": retailer_name,
                    }
                )

        out_file = path.join(input_folder, id + ".csv")
        order_files.append([out_file, json])
        write_to_csv(csv_headers, csv_data, out_file)

        return order_files


def process(order_files):
    init_app()

    for order_file, request_json in order_files:

        headers, data = csv_utils.read(order_file)

        logger.info(
            "\nprocessing data:\n" +
            "\n".join(list(map(lambda e: "* " + str(e), data)))
        )

        start_time = datetime.datetime.now()
        start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
        logger.info("punching start time: {}".format(start_time))

        item_count = len(data)
        unique_id = None

        index = -1
        is_customer_code_inserted = False

        curr_main_win = None
        enter_details_window = None
        interim_win = None

        for row in data:

            index = index + 1

            logger.info(separator)

            if is_window_exists("Error"):
                capture_screenshot("error")
                kill_exec(exe_name)
                return

            unique_id = row[0]
            customer_code = row[1]
            item_code = row[2]
            item_quantity = row[3]
            retailio_order_time = row[4]
            retailer_name = row[5]

            logger.info("* unique_id: {}".format(unique_id))
            logger.info("* customer_code: {}".format(customer_code))
            logger.info("* item_code: {}".format(item_code))
            logger.info("* item_quantity: {}".format(item_quantity))
            logger.info(
                "* retailio_order_time: {}".format(retailio_order_time, "\n"))
            logger.info("* customer_Name: {}".format(retailer_name))


            if curr_main_win == None:
                curr_main_win = (
                    Application(backend="uia")
                    .connect(
                        handle=find.find_window(
                            title_re=".*{}.*".format(main_title))
                    )
                    .window()
                    .children(title="Workspace")[-1]
                    .children()[0]
                )
                # curr_main_win.set_focus()

            if is_window_exists("Sales Order") and not is_customer_code_inserted:
                type_keys("^n")

                if interim_win == None:
                    interim_win = curr_main_win

                elm = fetch(
                    "c_cust_code",
                    get_text=False,
                    window=interim_win,
                )
                type_keys(customer_code + "~", elm)
                is_customer_code_inserted = True

                if is_window_exists("Message from Account Master"):
                    type_keys("~")

                elif is_window_exists("Customer Not Found!", 0):
                    type_keys("~")
                    time.sleep(1)
                    type_keys("{VK_ESCAPE}")

                    dict = {
                        "order_file": order_file,
                        "unique_id": unique_id,
                        "customer_code": customer_code,
                        "order_time": retailio_order_time,
                        "retailer_name": retailer_name,
                    }
                    update_customer_not_found(dict)
                    return

            if is_window_exists("Database error",1):
                logger.error("database error")
                type_keys("~")
                capture_screenshot("database-error")
                if alert_enabled:
                    mail.erp_db_error()
                    alert_by_sms("Sales order RPA Python ERP database error")
                kill_exec(exe_name)

            elif is_window_exists("Database Processing Error", 0):
                logger.error("database processing error")
                type_keys("~")
                capture_screenshot("database-processing-error")
                if alert_enabled:
                    mail.erp_db_error()
                    alert_by_sms("Sales order RPA Python ERP database error")
                kill_exec(exe_name)

            logger.info("entering order details => {}".format(row))

            if enter_details_window == None:
                enter_details_window = curr_main_win

            elm = fetch("c_item_code", get_text=False,
                        window=enter_details_window)
            type_keys(item_code + "~", elm)

            if is_window_exists("Item Not Found"):
                logger.error("item not found")
                # type_keys("~")
                # time.sleep(1)
                # type_keys("{VK_ESCAPE}")
                type_keys("~{VK_ESCAPE}")
                index = index - 1
                continue

            elif is_window_exists("Information!"):
                logger.error("information")
                type_keys("~")


            elif is_window_exists("Invalid Hsn", 0):
                logger.error("invalid hsn")
                # type_keys("~")
                # time.sleep(1)
                # type_keys("{VK_ESCAPE}")
                type_keys("~{VK_ESCAPE}")
                index = index - 1
                continue
            elif is_window_exists("Can't sale", 0):
                logger.error("cannot sale popup")
                # type_keys("~")
                # time.sleep(1)
                # type_keys("{VK_ESCAPE}")
                type_keys("~{VK_ESCAPE}")
                index = index - 1
                continue

            elm = fetch(
                "item_mst_n_qty_per_box", get_text=False, window=enter_details_window
            )
            type_keys(item_quantity + "~", elm)

            if is_window_exists("Warning"):
                type_keys("~")
            if is_window_exists("Maximum Sale Lot", 0):
                logger.info("maximum sales lot popup")
                type_keys("~")
            elif is_window_exists("Minimum Sale Lot", 0):
                logger.info("minimum sales lot popup")
                minimum_sales_lot = (
                    get_window("Minimum Sale Lot").descendants()[
                        2].window_text()
                )
                logger.info("minimum sales lot: ", minimum_sales_lot)

                min_item_code = minimum_sales_lot.split("For")[
                    0].split("of")[1]
                logger.info("minimum item code: {}".format(min_item_code))

                type_keys("~")
                time.sleep(1)
                type_keys(min_item_code)
                type_keys("~")

            if is_window_exists("Warning"):
                type_keys("~")

            type_keys("~")

        type_keys("^t")

        logger.info(separator)

        total_items = fetch("compute_1", window=enter_details_window)

        logger.info("total_items: {}".format(total_items))
        logger.debug("current index: {}".format(index))

        if str(total_items) != str(index + 1):
            kill_exec(exe_name)
            os.remove(order_file)
            return

        order_value = float(
            fetch(
                "ord_mst_n_total",
                window=interim_win,
            )
        )
        logger.info("order value: {}".format(order_value))

        type_keys("{VK_F12}")

        so_number = fetch("compute_2", window=interim_win)

        logger.info("so number: {}".format(so_number))

        if (
            unique_id == None
            or len(unique_id.strip()) == 0
            or so_number == None
            or len(so_number.strip()) == 0
        ):
            alert_by_sms(
                "Sales Order RPA Python SO number or Unique ID is NULL")
            continue

        if is_window_exists("Convert Order"):
            type_keys("n")

        if is_window_exists("Warning"):
            type_keys("~")

        end_time = datetime.datetime.now()
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
        logger.info("punching end time: {}".format(end_time))

        dict = {
            "order_file": order_file,
            "unique_id": unique_id,
            "customer_code": customer_code,
            "so_number": so_number,
            "order_value": order_value,
            "total_item": total_items,
            "order_time": retailio_order_time,
            "request_json": request_json,
            "start_time": start_time,
            "end_time": end_time,
            "item_count": item_count,
            "retailer_name": retailer_name,
        }
        update_status_to_processed(dict)
        time.sleep(1)


def main():
    while True:
        logger.info(separator)
        order_files = fetch_orders()
        if order_files == None or len(order_files) == 0:
            logger.error("no orders to process")
            time.sleep(2)
        else:
            start = time.time()
            try:
                process(order_files)
            except:
                pass
            finally:
                logger.info(
                    "total time taken: {} seconds".format(
                        round(time.time() - start, 2))
                )
                logger.info(separator)

main()
