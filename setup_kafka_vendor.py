import shutil
import os
import subprocess
import sys

def install_and_copy_six():
    # בדוק אם `six` מותקן, ואם לא התקן אותו
    try:
        import six
    except ImportError:
        print("Installing 'six' package...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "six"])

    # הגדרת נתיבי קבצים
    site_packages_path = next(p for p in sys.path if 'site-packages' in p)
    six_path = os.path.join(site_packages_path, 'six.py')
    kafka_vendor_path = os.path.join(site_packages_path, 'kafka', 'vendor')

    # בדוק אם `six.py` קיים ב-site-packages
    if not os.path.isfile(six_path):
        print("Error: 'six' package not found in site-packages.")
        return

    # צור את תיקיית `vendor` אם היא לא קיימת
    os.makedirs(kafka_vendor_path, exist_ok=True)

    # העתק את `six.py` לתוך `kafka/vendor`
    dest_six_path = os.path.join(kafka_vendor_path, 'six.py')
    shutil.copy(six_path, dest_six_path)

    print("Successfully copied 'six' to 'kafka.vendor'.")

# הפעלת הפונקציה
install_and_copy_six()
