import cv2
import time
import time
import easyocr
import numpy as np
from PIL import Image
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

def is_blue_shade(pixel):
    r, g, b = pixel
    return b > 100 and b > r + 20 and b > g + 20

def remove_blue_shades(image_path):
    img = Image.open(image_path)
    img = img.convert("RGB")
    pixels = img.load()

    for y in range(img.height):
        for x in range(img.width):
            if is_blue_shade(pixels[x, y]):
                pixels[x, y] = (255, 255, 255)

    return img

def getTextFromResult(results):
    if len(results) == 0:
        return None
    if(len(results[0]) == 0):
        return None
    return results[0][1][:3].lower()

# Main function to combine both processes and extract text
def process_image(input_path):
    img = remove_blue_shades(input_path)
    width, height = img.size
    left = int(width * 0.3)
    right = int(width * 0.7)
    img = img.crop((left, 0, right, height))
    image = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
    process_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    return process_image

def get_image(driver, fileName):
    iframe = driver.find_elements(By.XPATH, "//iframe")
    if(len(iframe) == 0):
        return 1
    driver.switch_to.frame(iframe[0])
    captcha_element = driver.find_elements(By.XPATH, "//img[contains(@src, 'kaptcha.jpg')]")
    if(len(captcha_element) == 0):
        return 1
    captcha_element[0].screenshot(fileName)

def get_cookies(driver, request_url):
    time.sleep(2)
    inputImage = 'kaptcha.png'
    reader = easyocr.Reader(['en'])
    tries = 0
    
    while True and tries < 10:
        try:
            time.sleep(2)
            ret = get_image(driver, inputImage)
            if ret == 1:
                driver.refresh()
                continue
            processed_image = process_image(inputImage)
            captcha_text = getTextFromResult(reader.readtext(processed_image))

            if not captcha_text:
                # print("No text found")
                driver.refresh()
                continue

            # print('Captcha text:', captcha_text)
            
            captcha_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//input[@name='captcha']"))
            )
            captcha_input.send_keys(captcha_text)

            cod_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//input[@name='cod']"))
            )
            cod_input.clear()
            cod_input.send_keys("94")

            submit_button = driver.find_element(By.XPATH, "//input[@name='B1' and @type='submit']")    
            
            submit_button.click()

            time.sleep(0.5)

            error_element = driver.find_elements(By.XPATH, "//font[contains(text(), 'Nu ati introdus codul de validare corect!')]")
            if len(error_element) > 0:
                # print("Error message found: Nu ati introdus codul de validare corect!")
                tries += 1
                driver.refresh()
                continue
            else:
                # print("Captcha solved and code submitted successfully")
                break
        except Exception as e:
            print(e)
            driver.refresh()
            tries += 1
            continue

    if(tries == 10):
        print("Failed to solve captcha")
        return None

    cookie_string = ''
    for request in driver.requests:
        if request_url in request.url:
            # print(f"Request URL: {request.url}")
            cookie_string = request.headers.get('Cookie')
            print(cookie_string)
            break
    
    return cookie_string

def main():
    chrome_options = Options()
    chrome_options.add_experimental_option("detach", True)
    driver = webdriver.Chrome(options=chrome_options)
    
    driver.get('https://mfinante.gov.ro/domenii/informatii-contribuabili/persoane-juridice/info-pj-selectie-dupa-cui')
    request_url = "https://mfinante.gov.ro/apps/infocodfiscal.html"
    cookies = get_cookies(driver, request_url)
    if(cookies == None):
        print("None")
    
    
if __name__ == "__main__":
    main()