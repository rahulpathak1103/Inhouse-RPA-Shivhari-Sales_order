import os, os.path, datetime, socket, codecs

# import rpa_lib.window_utils as win

import os.path as path
import shutil


class MainFolder:
    def __init__(self, distributor_name):
        def __create_directory_if_not_exists(dir_path, delete_contents=True):
            path_exists = os.path.exists(dir_path)
            if delete_contents and path_exists:
                shutil.rmtree(dir_path, ignore_errors=True)
                os.makedirs(dir_path, exist_ok=True)
            elif not path_exists:
                os.makedirs(dir_path, exist_ok=True)

        hostname = socket.gethostname()
        print("hostname: ", hostname)
        Userprofile = os.environ[r"USERPROFILE"]
        self.today = datetime.datetime.today()
        self.Day = self.today.strftime("%d")
        Month = self.today.strftime("%b")
        Year = self.today.strftime("%Y")
        DistributorName = distributor_name
        DocumentFolder = path.join(Userprofile, "Documents")
        ToolName = path.join(os.getcwd(), "purchase-order-files")
        ProcessFolder = path.join(ToolName, "SO Process")
        DistributorNameFolder = path.join(ProcessFolder, DistributorName)
        YearFolder = path.join(DistributorNameFolder, Year)
        MonthFolder = path.join(YearFolder, Month + "_" + Year)
        DayFolder = path.join(MonthFolder, self.Day)
        self.MasterFolder = path.join(MonthFolder, Month + "_Masterfolder")
        CurrentDayFolder = path.join(DistributorNameFolder, Year, Month, self.Day)
        self.InputFolder = path.join(DayFolder, "Input")
        self.LogsFolder = path.join(DayFolder, "Logs")
        self.OutputFolder = path.join(DayFolder, "Output")
        print("output folder => ", self.OutputFolder)
        self.InputApiData = path.join(self.OutputFolder, "InputGetApi")

        self.MacroFolder = os.path.join(DayFolder, "Macro")
        self.MacroSeperation = os.path.join(self.MacroFolder, "Macro Separation")
        self.SupplierSeparation = os.path.join(self.MacroFolder, "Supplier Separation")
        self.PONumber = os.path.join(self.OutputFolder, "PO Number")
        self.Macro = os.path.join(self.OutputFolder, "Macro")
        ERPOutputFiles = os.path.join(self.OutputFolder, "ERP Output Files")

        self.SalesOrderInputFolder = os.path.join(os.getcwd(), "sales-orders")

        __create_directory_if_not_exists(self.SalesOrderInputFolder)
        __create_directory_if_not_exists(DayFolder, True)
        __create_directory_if_not_exists(self.InputApiData)
        __create_directory_if_not_exists(self.OutputFolder)
        __create_directory_if_not_exists(self.LogsFolder)
        __create_directory_if_not_exists(self.MacroFolder)
        __create_directory_if_not_exists(self.MacroSeperation)
        __create_directory_if_not_exists(self.SupplierSeparation)
        __create_directory_if_not_exists(self.PONumber)
        __create_directory_if_not_exists(self.Macro)
        __create_directory_if_not_exists(ERPOutputFiles)
        __create_directory_if_not_exists(self.MasterFolder)
