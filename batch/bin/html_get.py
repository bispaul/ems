"""
Gets the html from a website and other objects whihc needs to be fetched
"""
import mechanize
from BeautifulSoup import BeautifulSoup


BROWSER_HEADER = [('User-agent', 'Mozilla/5.0 (Windows NT 6.1; WOW64) \
                    AppleWebKit/537.4 (KHTML, like Gecko) \
                    Chrome/22.0.1229.79 Safari/537.4')]


def getbrowser():
    """
    Create Browser Object
    """
    browser = mechanize.Browser()
    browser.set_handle_robots(False)
    browser.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(),
                               max_time=1)
    browser.addheaders = BROWSER_HEADER
    return browser


def get_url(browser, url, eventtarget=None):
    """
    Get all the revision number from the nrldc url passed
    """
    response = browser.open(url)
    if not eventtarget:
        return (browser, response)
    for events in eventtarget:
        response.set_data(BeautifulSoup(response.get_data()).prettify())
        browser.set_response(response)
        browser.select_form(nr=0)
        browser.set_all_readonly(False)
        browser["__EVENTTARGET"] = events[0]
        browser["__EVENTARGUMENT"] = ''
        if events[1] != '':
            if events[0] == "txtStartDate":
                browser[events[0]] = events[1]
            else:
                browser[events[0]] = [str(events[1])]
        if events[0] == 'download':
            for control in browser.form.controls:
                if control.type == "submit":
                    control.disabled = True
        response = browser.submit()
    return (browser, response)


#finding from the state selection dropdown the id value of the states
def controlitem_fetch(browser, url, controlname, controllist=None):
    """
    Get the control item values
    """
    controlitem = {}
    browser = get_url(browser, url, controllist)[0]
    browser.form = list(browser.forms())[0]
    control = browser.form.find_control(controlname)
    if control.type == "select":  # means it's class ClientForm.SelectControl
        for item in control.items:
            controlitem[int(item.name)] = \
                       [label.text for label in item.get_labels()][0]
    return controlitem
