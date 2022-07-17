"""
Parses all HTML tables found at the given url. Missing data or those
without text content will be replaced with the missingCell string.
Returns a list of lists of strings, corresponding to rows within all
found tables.
"""
import lxml.html
import logging


def parse(url, xpath, missingcell="NA"):
    """
    Parses all HTML tables found at the given url. Missing data or those
    without text content will be replaced with the missingCell string.
    Returns a list of lists of strings, corresponding to rows within all
    found tables.
    """
    logger = logging.getLogger("html_tab_parse.parse")
    doc = lxml.html.document_fromstring(url)
    tablelist = doc.xpath(xpath)
    datalist = []
    for table in tablelist:
        datalist.append(parsetable(table, missingcell))
        logger.debug(datalist)
    return datalist


#parsing the table out of the html seeking out data between td and tr html tags
def parsetable(table, missingcell):
    """
    Parses the individual HTML table, returning a list of its rows.
    """
    rowlist = []
    for row in table.xpath('.//tr'):
        collist = []
        cells = row.xpath('.//td') + row.xpath('.//th')
        for cell in cells:
            # The individual cell's content
            content = cell.text_content().encode("utf8")
            if content == "":
                content = missingcell
            collist.append(content)
        rowlist.append(collist)
    return rowlist
