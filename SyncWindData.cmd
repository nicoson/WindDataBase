::========  set time stamp
echo off
set stockJob=main_stock.py
set futureJob=main_contract.py
set futureMCJob=main_maincontract.py
set futureIndexJob=main_futureindex.py
set /A startStockSync=1
set /A startFutureSync=4
set /A startMCUpdate=8
set /A startIndexUpdate=9
set /A closeJobSync=10
set /A status=1


cd C:\Project\WindDataBase

:label-loop

    set /A currentTime="%time:~,2%"
    echo %time%

    ::  =====   get into the cold period, start the stock sync job
    if %currentTime% geq %startStockSync% (
        if %currentTime% lss %startFutureSync% (
            if %status% equ 1 (
                set /A status=0
                echo "start stock sync job"
                echo status changed to 0
                python %stockJob%
            )
        )
    )

    ::  =====   get into the cold period, start the future sync job
    if %currentTime% geq %startFutureSync% (
        if %currentTime% lss %startMCUpdate% (
            if %status% equ 0 (
                set /A status=1
                echo "start future sync job"
                echo status changed to 0
                python %futureJob%
            )
        )
    )

    ::  =====   get into the cold period, start the main contract job
    if %currentTime% geq %startMCUpdate% (
        if %currentTime% lss %startIndexUpdate% (
            if %status% equ 1 (
                set /A status=0
                echo "start update main contract job"
                echo status changed to 0
                python %futureMCJob%
            )
        )
    )

    ::  =====   get into the cold period, start the future index job
    if %currentTime% geq %startIndexUpdate% (
        if %currentTime% lss %closeJobSync% (
            if %status% equ 0 (
                set /A status=1
                echo "start update contract index job"
                echo status changed to 0
                python %futureIndexJob%
            )
        )
    )

    ::  =====   close the stock sync job

    echo ================ %status%

    ::  =====   sleep for 10 mins - 10x60+1
    ping -n 601 127.1 >nul

goto label-loop