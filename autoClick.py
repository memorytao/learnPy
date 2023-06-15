from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By


username = "chinnawat"
password = "Taoppbk2022!"

options = webdriver.ChromeOptions()

driver = webdriver.Chrome("chromedriver",options=options)
# head to github login page
driver.get("https://clickcounter.io/")
# find username/email field and send the username itself to the input field
# driver.find_element("id", "inputUsername").send_keys(username)
# find password input field and insert password as well
# driver.find_element("id", "inputPassword").send_keys(password)
# click login button

for x in range(1000):
    driver.find_element(By.ID, "start").click()

input(' any key to exit')

