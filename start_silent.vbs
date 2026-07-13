Set WshShell = CreateObject("Wscript.Shell")
proj = "d:\zhuye\OneDrive\Desktop\xiangmu\live-ecommerce-analysis"
WshShell.Run "cmd /c python " & proj & "\crawler\douyin_full_crawler.py > nul 2>&1", 0, False
' danmaku crawler needs to run from crawler directory
WshShell.CurrentDirectory = proj & "\crawler"
WshShell.Run "cmd /c python douyin_multi_room.py > nul 2>&1", 0, False
WshShell.CurrentDirectory = proj
WshShell.Run "cmd /c python " & proj & "\pipeline\kafka_to_mysql.py > nul 2>&1", 0, False
WshShell.Run "cmd /c python " & proj & "\pipeline\realtime_processor.py > nul 2>&1", 0, False
WshShell.Run "cmd /c python " & proj & "\backend\app.py > nul 2>&1", 0, False
