#encoding=utf-8

import requests
import concurrent.futures
from parsel import Selector
import os
import csv


alreadyscrapped = []

try:
      with open("ITJobs.csv","r") as r:
            reader = csv.reader(r)
            for line in reader:
                  alreadyscrapped.append(line[0])
except:
      pass

def getdata(link):
      print(link)
      response = Selector(text=requests.get(link).text)
      url = link
      title = response.xpath('.//*[@class="icl-u-xs-mb--xs icl-u-xs-mt--none jobsearch-JobInfoHeader-title"]/text()').extract_first()
      company = response.xpath('.//*[@class="jobsearch-InlineCompanyRating icl-u-xs-mt--xs  jobsearch-DesktopStickyContainer-companyrating"]/div/a/text()').extract_first()
      location = response.xpath('.//*[@class="icl-IconFunctional icl-IconFunctional--location icl-IconFunctional--md"]/following-sibling::span/text()').extract_first()
      job_type = response.xpath('.//*[@class="icl-IconFunctional icl-IconFunctional--jobs icl-IconFunctional--md"]/following-sibling::span/text()').extract_first()
      try:
            description = ' '.join([i for i in response.xpath('.//*[@id="jobDescriptionText"]//text()').extract() if i.strip()])
      except:
            description = ''


      with open("ITJobs.csv","a") as f:
            writer = csv.writer(f)
            writer.writerow([url,title,company,location,job_type,description])
            print([url,title,company,location,job_type,description])
      alreadyscrapped.append(link)

def getdatas(link):
      print(link)
      response = Selector(text=requests.get(link).text)
      links = list(set(['https://nz.indeed.com'+str(i) for i in response.xpath('.//*[@class="title"]/a/@href').extract()])-set(alreadyscrapped))

      with concurrent.futures.ProcessPoolExecutor(max_workers=2) as f:
          a = {
                f.submit(getdata,link)
                for link in links
          }

      nextlink = response.xpath('.//*[@rel="next"]/@href').extract_first()
      if nextlink:
            getdatas('https://nz.indeed.com'+str(nextlink))




if __name__=="__main__":

      if 'IT_Jobs.csv' not in os.listdir(os.getcwd()):
            with open("ITJobs.csv","a") as f:
                  writer = csv.writer(f)
                  writer.writerow(['url','title','company','location','job_type','description'])

      IT_JOBS = ['IT',
            'Hardware Engineer',
            'Network Administrator',
            'Data Architect',
            'Solutions Architect',
            'Computer Network Architect',
            'Computer Technical Support Specialist',
            'Site Reliability Engineer',
            'Computer Systems Analyst',
            'Software Engineer',
            'User Interface Designer',
            'Database Administrator',
            'Business Intelligence Developer',
            'Information Technology Manager',
            'Data Scientist',
            'Development Operations (DevOps) Engineer',
            'Applications Architect',
            'Cloud Solutions Architect',
            'Web Developer',
            'Information Security Analyst',
            'Mobile Application Developer']

      with concurrent.futures.ProcessPoolExecutor(max_workers=2) as f:
          a = {
              f.submit(getdatas,'https://nz.indeed.com/jobs?q='+str(data).replace(' ','+'))
              for data in IT_JOBS
          }

      # for data in IT_JOBS:
      #       getdatas('https://nz.indeed.com/jobs?q='+str(data).replace(' ','+'))
      #       break
