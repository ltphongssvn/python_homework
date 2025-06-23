#!/usr/bin/env python3
# chromedriver_ssl_test.py
"""
ChromeDriver SSL Testing and Debugging Tool

This module provides comprehensive testing for diagnosing SSL-related
issues when using Selenium with Chrome for web scraping. It performs
incremental testing with different Chrome configurations to identify
the source of SSL connection problems.
"""

import logging
import subprocess
import sys
import time
from types import ModuleType
from typing import Optional

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

# Handle platform-specific imports for cross-platform compatibility.
# The `Optional` type hint resolves a MyPy assignment error.
winreg: Optional[ModuleType]
try:
    import winreg  # Windows registry access for Chrome version detection
except ImportError:
    winreg = None  # Not available on non-Windows platforms


def setup_logging():
    """
    Configure comprehensive logging to capture all relevant debugging info.

    This sets up handlers for both file-based logs and console output.
    Detailed logging helps us understand exactly what happens during
    SSL handshake attempts and ChromeDriver operations.
    """
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("chromedriver_ssl_debug.log"),
            logging.StreamHandler(),
        ],
    )

    # Enable detailed logging for network-related libraries
    logging.getLogger("urllib3").setLevel(logging.DEBUG)
    logging.getLogger("selenium").setLevel(logging.DEBUG)


def get_chrome_version():
    """
    Detect the installed Chrome browser version using platform-specific methods.

    Knowing the Chrome version is crucial for SSL debugging, as different
    versions may have varying security policies and TLS support.

    Returns:
        str: Chrome version string or descriptive error message.
    """
    try:
        if sys.platform == "win32" and winreg is not None:
            # Windows: Try registry lookup first (fast and reliable)
            try:
                key = winreg.OpenKey(
                    winreg.HKEY_CURRENT_USER,
                    r"Software\Google\Chrome\BLBeacon",
                )
                version, _ = winreg.QueryValueEx(key, "version")
                winreg.CloseKey(key)
                return version
            except (FileNotFoundError, OSError, PermissionError) as reg_err:
                # Fall through to subprocess approach if registry fails
                logging.debug("Registry lookup failed: %s", reg_err)

            # Windows fallback: Execute Chrome directly
            try:
                result = subprocess.run(
                    [
                        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                        "--version",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=True,
                )
                # Output: "Google Chrome 118.0.5993.70"
                return result.stdout.strip().split()[-1]
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                FileNotFoundError,
                IndexError,
            ) as sub_err:
                logging.debug("Windows subprocess failed: %s", sub_err)

        elif sys.platform == "darwin":  # macOS
            try:
                result = subprocess.run(
                    [
                        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                        "--version",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=True,
                )
                return result.stdout.strip().split()[-1]
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                FileNotFoundError,
                IndexError,
            ) as mac_err:
                logging.debug("macOS Chrome detection failed: %s", mac_err)

        else:  # Linux and other Unix-like systems
            try:
                result = subprocess.run(
                    ["google-chrome", "--version"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=True,
                )
                return result.stdout.strip().split()[-1]
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                FileNotFoundError,
                IndexError,
            ) as linux_err:
                logging.debug("Linux Chrome detection failed: %s", linux_err)

        return "Chrome not found or version undetectable"

    except MemoryError:
        return "System resource error during Chrome detection"
    # pylint: disable=try-except-raise
    except KeyboardInterrupt:
        # Allow user interruption to propagate
        raise
    except UnicodeDecodeError as encoding_error:
        return f"Encoding error in Chrome output: {encoding_error}"


def test_chromedriver_ssl_incrementally():
    """
    Test ChromeDriver SSL with progressively more complex configurations.

    This incremental approach helps isolate SSL issues by starting with a
    simple configuration and gradually adding complexity, allowing us to
    identify which settings resolve specific SSL challenges.
    """
    print("=== ChromeDriver SSL Diagnostic Test ===")
    chrome_version = get_chrome_version()
    print(f"Chrome Browser Version: {chrome_version}")

    # Test 1: Minimal Chrome configuration to establish baseline
    print("\nTest 1: Basic Chrome with minimal security settings")
    options = Options()
    options.add_argument("--log-level=0")  # Enable verbose Chrome logging
    options.add_experimental_option("w3c", True)  # Use W3C standard

    driver = None
    try:
        service = Service()
        driver = webdriver.Chrome(service=service, options=options)
        print("✓ ChromeDriver initialized successfully")

        # Test with a known-good site to verify basic SSL functionality
        print("Testing SSL with known-good site (google.com)...")
        driver.set_page_load_timeout(15)  # Set reasonable timeout
        driver.get("https://www.google.com")
        print("✓ Google.com loaded successfully - basic SSL is working")

        # Now attempt to load the target site
        print("Testing with baseball-almanac.com...")
        driver.get("https://www.baseball-almanac.com/yearmenu.shtml")
        print("✓ Baseball-almanac.com loaded successfully!")
        print(f"Page title: {driver.title}")

    except TimeoutException as timeout_err:
        print(f"✗ Test 1 timeout: {timeout_err}")
        print("  This suggests the site is slow or has connection issues.")
    except WebDriverException as driver_err:
        print(f"✗ Test 1 WebDriver error: {driver_err}")
        print("  This indicates a ChromeDriver or browser-level problem.")
    except MemoryError:
        print("✗ Test 1 failed: Insufficient system memory for browser.")
    # pylint: disable=try-except-raise
    except KeyboardInterrupt:
        print("Test 1 interrupted by user")
        raise  # Re-raise to allow program termination
    finally:
        if driver:
            try:
                driver.quit()
            except WebDriverException as cleanup_error:
                logging.warning(
                    "Error during driver cleanup: %s", cleanup_error
                )

    # Test 2: Chrome with SSL security bypasses
    print("\nTest 2: Chrome with SSL certificate bypass flags")
    options = Options()

    # Add comprehensive SSL bypass arguments to tell Chrome to ignore issues
    ssl_bypass_args = [
        "--ignore-certificate-errors",
        "--ignore-ssl-errors",
        "--allow-insecure-localhost",
        "--disable-web-security",
        "--allow-running-insecure-content",
        "--ignore-certificate-errors-spki-list",
        "--ignore-certificate-errors-primary-name",
        "--log-level=0",
    ]
    for arg in ssl_bypass_args:
        options.add_argument(arg)

    # Enable performance logging to capture network events
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    driver = None
    try:
        service = Service()
        driver = webdriver.Chrome(service=service, options=options)
        print("✓ ChromeDriver started with SSL bypass configuration")

        # Enable Chrome DevTools for detailed monitoring
        driver.execute_cdp_cmd("Network.enable", {})

        print("Loading baseball-almanac.com with SSL bypasses...")
        start_time = time.time()
        driver.set_page_load_timeout(20)
        driver.get("https://www.baseball-almanac.com/yearmenu.shtml")
        load_time = time.time() - start_time

        print(f"✓ Page loaded successfully in {load_time:.2f} seconds")
        print(f"Page title: {driver.title}")

        # Analyze performance logs for SSL-related events
        try:
            logs = driver.get_log("performance")
            ssl_keywords = ["ssl", "tls", "certificate", "handshake", "cipher"]
            ssl_events = [
                entry
                for entry in logs
                if any(
                    k in entry.get("message", "").lower() for k in ssl_keywords
                )
            ]

            if ssl_events:
                print(f"\nCaptured {len(ssl_events)} SSL-related events:")
                # Show the most relevant events (limited to avoid spam)
                for event in ssl_events[:3]:
                    timestamp = event.get("timestamp", "Unknown")
                    # FIX: Correctly use 'event' instead of 'entry'
                    message = event.get("message", "")[:100]
                    print(f"  [{timestamp}] {message}...")
            else:
                print("\nNo specific SSL events in performance logs")

        except WebDriverException as log_error:
            print(f"Could not retrieve performance logs: {log_error}")

    except TimeoutException as timeout_err:
        print(f"✗ Test 2 timeout: {timeout_err}")
        print("  SSL bypasses didn't resolve the timeout issue.")
    except WebDriverException as driver_err:
        print(f"✗ Test 2 WebDriver error: {driver_err}")
        print("  Issue persists even with SSL bypasses.")
    except MemoryError:
        print("✗ Test 2 failed: Insufficient system memory for browser.")
    # pylint: disable=try-except-raise
    except KeyboardInterrupt:
        print("Test 2 interrupted by user")
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except WebDriverException as cleanup_error:
                logging.warning(
                    "Error during driver cleanup: %s", cleanup_error
                )

    # Test 3: Advanced SSL error handling via Chrome DevTools Protocol
    print("\nTest 3: Advanced SSL error interception and handling")
    options = Options()

    # Use a focused set of the most effective SSL bypass arguments
    essential_args = [
        "--ignore-certificate-errors",
        "--ignore-ssl-errors",
        "--disable-web-security",
        "--allow-running-insecure-content",
    ]
    for arg in essential_args:
        options.add_argument(arg)

    # Reduce Chrome's internal logging noise
    options.add_experimental_option("excludeSwitches", ["enable-logging"])

    driver = None
    try:
        service = Service()
        driver = webdriver.Chrome(service=service, options=options)
        print("✓ ChromeDriver started with advanced SSL handling")

        # Enable DevTools domains for comprehensive monitoring
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Security.enable", {})

        # Configure Chrome to automatically handle certificate errors
        driver.execute_cdp_cmd(
            "Security.setOverrideCertificateErrors", {"override": True}
        )

        print("Loading page with comprehensive SSL error handling...")
        driver.set_page_load_timeout(25)
        driver.get("https://www.baseball-almanac.com/yearmenu.shtml")

        print(f"✓ Page loaded successfully: {driver.title}")

        # Get security state information if available
        try:
            state = driver.execute_cdp_cmd("Security.getSecurityState", {})
            security_level = state.get("securityState", "unknown")
            print(f"Final security state: {security_level}")
        except WebDriverException as security_error:
            print(f"Could not get security state: {security_error}")

    except TimeoutException as timeout_err:
        print(f"✗ Test 3 timeout: {timeout_err}")
        print("  Advanced SSL handling did not resolve connection issues.")
    except WebDriverException as driver_err:
        print(f"✗ Test 3 WebDriver error: {driver_err}")
        print("  Issue persists despite all SSL configurations.")
    except MemoryError:
        print("✗ Test 3 failed: Insufficient system memory for browser.")
    # pylint: disable=try-except-raise
    except KeyboardInterrupt:
        print("Test 3 interrupted by user")
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except WebDriverException as cleanup_error:
                logging.warning(
                    "Error during driver cleanup: %s", cleanup_error
                )

    print("\n=== SSL Testing Complete ===")
    print("Check 'chromedriver_ssl_debug.log' for detailed information.")


def main():
    """
    Main entry point that sets up logging and runs the SSL tests.

    This provides a clean separation between setup and execution, making
    the code more maintainable.
    """
    setup_logging()
    try:
        test_chromedriver_ssl_incrementally()
    except KeyboardInterrupt:
        print("\nSSL testing interrupted by user. Exiting gracefully.")
        sys.exit(0)


if __name__ == "__main__":
    main()
