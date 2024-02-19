import pandas as pd
from time import sleep
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
import os
import chromedriver_autoinstaller
from selenium.webdriver.common.action_chains import ActionChains

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.ui import Select
opt = webdriver.ChromeOptions()
opt.add_argument("--start-maximized")
opt.add_experimental_option("excludeSwitches", ["disable-popup-blocking"])
# opt.add_argument("--lang=en")
# chromedriver_autoinstaller.install()