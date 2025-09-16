runtype = 'Weekly'

def finish():
    if runtype == 'Weekly':
        %run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/TrueCar_UNIX_Commands_Sales_20220117"
        %run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/TrueCar_Weekly_20220117"
        %run "/Workspace/aarp_sas/aarp_projects/twalters/Data_Processing/TrueCar_UNIX_Commands_Prospects_20220117"

finish()
#End-DBShift