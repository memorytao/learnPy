from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


options = Options()
driver = webdriver.Chrome()

driver.get("https://www.google.com/")
driver.find_element(By.NAME, "q").send_keys("python")
driver.find_element(By.NAME, "btnK").submit()


input('enter to exit')
driver.close()
