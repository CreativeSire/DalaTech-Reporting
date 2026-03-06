import subprocess, sys, os

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'install_output.txt')
log = open(OUT, 'w', encoding='utf-8')

packages = [
    'flask', 'pandas', 'openpyxl', 'xlrd',
    'reportlab', 'matplotlib', 'Pillow',
    'gspread', 'google-auth'
]

log.write('Installing packages into: ' + sys.executable + '\n\n')
log.flush()

result = subprocess.run(
    [sys.executable, '-m', 'pip', 'install'] + packages,
    capture_output=True, text=True
)
log.write(result.stdout)
log.write(result.stderr)
log.write('\nExit code: ' + str(result.returncode) + '\n')
log.close()
print('Done — see install_output.txt')
