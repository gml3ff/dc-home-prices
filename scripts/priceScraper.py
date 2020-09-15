import requests
import time
import logging
import json_log_formatter
import xml.etree.ElementTree as ET
from datadog import statsd
from datadog import initialize
from ddtrace import tracer
from ddtrace.constants import ANALYTICS_SAMPLE_RATE_KEY
from ddtrace.helpers import get_correlation_ids

options = {
    'api_key':'<your-api-key>',
    'app_key':'<your-app-key>'
}

initialize(**options)

# create master list of all listings
master_list = []

# configure logging

formatter = json_log_formatter.JSONFormatter()
json_handler = logging.FileHandler(filename='/var/log/scraper.json')
json_handler.setFormatter(formatter)
logger = logging.getLogger('my_json')
logger.addHandler(json_handler)
logger.setLevel(logging.INFO)

while True:
  @tracer.wrap(name='searchHomesNW', service='home-price-scraper')
  def searchHomesNW():
    span = tracer.current_span()
    span.set_tag(ANALYTICS_SAMPLE_RATE_KEY, True)
    span.set_tag("region", "northwest_dc")

    # search all listings in D.C. with 'NW' in address
    url = 'http://www.zillow.com/webservice/GetSearchResults.htm?zws-id=X1-ZWz17wfvdjpczv_5i268&address=*NW*&citystatezip=Washington%2C+DC'

    # creates HTTP response object from URL
    resp = requests.get(url)

    # save xml response
    with open('homesnw.xml','wb') as f:
      f.write(resp.content)

  @tracer.wrap(name='searchHomesNE', service='home-price-scraper')
  def searchHomesNE():
    span = tracer.current_span()
    span.set_tag(ANALYTICS_SAMPLE_RATE_KEY, True)
    span.set_tag("region", "northeast_dc")

    # search all listings in D.C. with 'NE' in address
    url = 'http://www.zillow.com/webservice/GetSearchResults.htm?zws-id=X1-ZWz17wfvdjpczv_5i268&address=*NE*&citystatezip=Washington%2C+DC'

    # creates HTTP response object from URL
    resp = requests.get(url)

    # save xml response
    with open('homesne.xml','wb') as f:
      f.write(resp.content)

  @tracer.wrap(name='searchHomesSE', service='home-price-scraper')
  def searchHomesSE():
    span = tracer.current_span()
    span.set_tag(ANALYTICS_SAMPLE_RATE_KEY, True)
    span.set_tag("region", "southeast_dc")

    # search all listings in D.C. with 'SE' in address
    url = 'http://www.zillow.com/webservice/GetSearchResults.htm?zws-id=X1-ZWz17wfvdjpczv_5i268&address=*SE*&citystatezip=Washington%2C+DC'

    # creates HTTP response object from URL
    resp = requests.get(url)

    # save xml response
    with open('homesse.xml','wb') as f:
      f.write(resp.content)
  
  @tracer.wrap(name='searchHomesSW', service='home-price-scraper')
  def searchHomesSW():
    span = tracer.current_span()
    span.set_tag(ANALYTICS_SAMPLE_RATE_KEY, True)
    span.set_tag("region", "southwest_dc")

    # search all listings in D.C. with 'SW' in address
    url = 'http://www.zillow.com/webservice/GetSearchResults.htm?zws-id=X1-ZWz17wfvdjpczv_5i268&address=*SW*&citystatezip=Washington%2C+DC'

    # creates HTTP response object from URL
    resp = requests.get(url)

    # save xml response
    with open('homessw.xml','wb') as f:
      f.write(resp.content)

  @tracer.wrap(name='parseXML', service='home-price-scraper')
  def parseXML(xmlfile):
    span = tracer.current_span()
    span.set_tag(ANALYTICS_SAMPLE_RATE_KEY, True)

    # create element tree object
    tree = ET.parse(xmlfile)

    # get root element
    root = tree.getroot()

    # create empty list for listings
    listings = []

    # iterate over listings
    for result in root.findall('./response/results/result'):

      # empty listings dictionary
      listings_dict = {}

      # iterate child elements of results
      for child in result:
      # save text as UTF8
        if "zpid" in str(child):
          listings_dict[child.tag] = child.text
        elif "links" in str(child):
          listings_dict["link"] = child[0].text
        elif "address" in str(child):
          listings_dict[child.tag] = child[0].text
          listings_dict["zipcode"] = child[1].text
        elif "zestimate" in str(child):
          if child[0].text is not None:
            listings_dict["amount"] = child[0].text
          listings_dict["last_updated"] = child[1].text
        elif "localRealEstate" in str(child):
          listings_dict["neighborhood"] = child[0].attrib["name"]


      # append listings dictionary to listings list
      listings.append(listings_dict)

      # add to master list if listing doesn't already exist
      if listings_dict not in master_list and "amount" in listings_dict:
        master_list.append(listings_dict)
        zipcode = 'zipcode:'+listings_dict["zipcode"]
        neighborhood = 'neighborhood:'+listings_dict["neighborhood"]
        statsd.event('New Listing!', listings_dict["address"]+'\nPrice: $'+listings_dict["amount"]+'\nDirect Link Here: '+listings_dict["link"],tags=[zipcode,neighborhood])

    # return listings list
    return listings

  @tracer.wrap(name='getPrices', service='home-price-scraper')
  def getPrices(listings):
    span = tracer.current_span()
    span.set_tag(ANALYTICS_SAMPLE_RATE_KEY, True)
    trace_id, span_id = get_correlation_ids()
    dd_apm_url = "https://app.datadoghq.com/apm/trace/" + str(trace_id)

    for listing in listings:
        zipcode = 'zipcode:'+listing["zipcode"]
        neighborhood = 'neighborhood:'+listing["neighborhood"]
        if "amount" in listing.keys():
          #print listing["amount"]
          statsd.gauge('home.price',int(listing["amount"]),tags=[zipcode,neighborhood])
          logger.info('New listing for $%s in %s',listing["amount"],listing["neighborhood"],extra={'neighborhood': listing["neighborhood"],'amount': listing["amount"], 'service': 'home-price-scraper', 'dd_apm_url': dd_apm_url})

  @tracer.wrap(name='main', service='home-price-scraper')
  def main():
    trace_id, span_id = get_correlation_ids()
    span = tracer.current_span()
    span.set_tag(ANALYTICS_SAMPLE_RATE_KEY, True)
    splunk_url = "http://ec2-35-174-137-140.compute-1.amazonaws.com:8000/en-US/app/search/search?q=search%20index%3D_*%20OR%20index%3D*%20sourcetype%3Dscraper_app_logs%20service%3D%22home-price-scraper%22%20dd.trace_id%3D" + str(trace_id) + "&display.page.search.mode=verbose&dispatch.sample_ratio=1&workload_pool=&earliest=%40mon&latest=now&sid=1588268934.2225"
    span.set_tag('splunk_url', splunk_url)
    # retrieve results from web to update existing file
    searchHomesNW()
    searchHomesSW()
    searchHomesNE()
    searchHomesSE()

    # parse xml file
    listingsnw = parseXML('homesnw.xml')

    #print listings
    getPrices(listingsnw)

    # parse xml file
    listingsne = parseXML('homesne.xml')

    #print listings
    getPrices(listingsne)

    # parse xml file
    listingsse = parseXML('homesse.xml')

    #print listings
    getPrices(listingsse)

    # parse xml file
    listingssw = parseXML('homessw.xml')

    #print listings
    getPrices(listingssw)

  if __name__ == "__main__":
    # calling main function
    main()
  time.sleep(500)
