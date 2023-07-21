import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


options = Options()
options.debugger_address = "localhost:9222"
driver = webdriver.Chrome(executable_path='../chromedriver_win32/chromedriver.exe',options=options)

# options.debugger_address = "localhost:9223"
# driver = webdriver.Edge(executable_path="../edgedriver_win64/msedgedriver.exe",options=options)

print(driver.get_cookies())
driver.get('https://www.google.com/')
driver.find_element(By.NAME, "q").send_keys('blognone')
time.sleep(1)
driver.find_element(By.NAME, "btnK").submit()
print(driver.get_cookies())


input("enter to exit")
driver.quit()
