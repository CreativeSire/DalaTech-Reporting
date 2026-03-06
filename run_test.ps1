$pyExe = "C:\Users\HomePC\AppData\Local\Programs\Python\Python312\python.exe"
$script = "C:\Users\HomePC\Desktop\Sales Report DashBoard\test_phase3.py"
$output = "C:\Users\HomePC\Desktop\Sales Report DashBoard\test_output.txt"
& $pyExe $script 2>&1 | Tee-Object -FilePath $output
