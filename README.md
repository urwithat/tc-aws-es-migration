TC AWS Dynamo to ES Migtrator
=============================

<dl>
  <dt>Description</dt>
  <dd>Tool to help pull data from Dynamo to Elasticsearch</dd>
  <dt>Technology</dt>
  <dd>Console based application, ability to run in multiple segmentas to speed up time of execution.</dd>
</dl>

---

##  Features Covered
- Query / Scan data from DynamoDB
- Handling DynamoDB LastEvaluatedKey for continous invokation
- Handling DynamoDB Segment & TotalSegments
- Create, Add, Search, Drop data in Elastic search
- Handling exceptions to re-invoke work

##  Know Issues
- Troughput Issue in Dynaomodb - Add workaround
- Elasticsearch instace unavaiable - Reduced the number of Segments 

##  Good To Have Features
- NA

> Note: 
> + All commands are in par with MacOS
> + Run commands from root folder /Workspace/../tc-aws-es-migration-repository

---

##  Running the Web Application
####  Software Stack To Run the Executable
| No | Software                 | Tested on Version |
| -- |:------------------------:| -----------------:|
| 01 | *node*                   | v8.9.2            |
| 02 | *npm*                    | v5.5.1            |

####  Start Up Web Application
- Go to folder
```
/Workspace/../tc-aws-es-migration-repository
```
- Pull master branch
```
git clone -b master https://github.com/urwithat/tc-aws-es-migration.git
```
- Go to folder
```
cd tc-aws-es-migration/
```
- run the application
```
npm i; node index.js
```
> **The Application should have executed**