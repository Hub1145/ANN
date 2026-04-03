import asyncio
from playwright.async_api import async_playwright
import time

async def verify():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        try:
            # Go to the dashboard
            await page.goto("http://127.0.0.1:3000")
            print("Dashboard loaded.")

            # Check for title
            title = await page.title()
            print(f"Title: {title}")

            # Click on 'Console Log' tab to make #consoleOutput visible
            await page.click("div[data-tab-target='#logTab']")
            print("Clicked Console Log tab.")

            # Wait for console output to be visible
            await page.wait_for_selector("#consoleOutput", state="visible", timeout=5000)
            print("Console output is visible.")

            # Click Add Trade button
            await page.click("#btnAddTrade")
            print("Clicked 'Add New Trade' button.")

            # Wait a bit for logs
            await asyncio.sleep(2)

            # Check logs content
            logs = await page.inner_text("#consoleOutput")
            print(f"Latest Logs:\n{logs}")

            await page.screenshot(path="verification_dashboard_final.png")
            print("Screenshot saved.")

        except Exception as e:
            print(f"Verification failed: {e}")
            await page.screenshot(path="verification_error.png")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(verify())
