# Talend HVR Command Line Interface #
## Version: 1.3.0 ##
### Author: Thomas Bennett <tbennett@talend.com> ###

The executables in this repository where built to support the following systems
* Windows <tcli.exe>
* Mac <tcli>
* Linux/Unix <tcli>

The log4j.properties file needs be downloaded as well and placed in the same directory
as the executable that you choose.

The following parameters can be used with the executable

| Command  | Description |
| --- | --- |
| -h,--help  | show help
| -r,--region \<arg> | Talend Cloud Region [AWS_USA_EAST, AWS_EMEA, AWS_APAC, AZURE_USA_WEST]
| -t,--token \<arg> | Talend Cloud Token
| -te,--tokenenv \<arg> | Talend Cloud Token in Environment Variable
| -w,--wait | Will block any other commands from executing until Talend job is completed
| -e,--environment \<arg> | The environment the job is in. If not used name will be `default`
| -cv,--contextvariables \<arg> | Context Variables to pass. EX: name1=value1;name2=value2
| -hm,--hvrmanifest \<arg> | Directory path to the HVR Manifest files
| -tm,--tlndmanifest \<arg> | File path to the Talend Manifest file
| -v,--version | Product Version



# HVR Example
In working in conjuction with HVR to integrate their CDC platform to Talend Cloud we utilized the following script. The tcli executable
and log4j.properties was placed in the <$HVR_HOME>\lib\agent directory allow with the following script

`IF "%1%" == "integ_end" (
  tcli -t <TOKEN> -r AWS_USA_EAST -w -hm <path to hvr manifest dir> -tm <path to talend manifest file>
 )`

# HVR Manifest Example
`
{
    "channel": "hvrdemo",
    "cycle_begin": "2020-02-04T19:39:56Z",
    "cycle_end": "2020-02-04T19:39:57Z",
    "initial_load": false,
    "integ_loc": {
        "name": "tgt"
    },
    "tables": [
        "dm51_order",
        "dm51_product",
        "dm51_test"
    ]
}
`

# Talend Manifest Example
The Talend Manifest file is an array of mapping objects each mapping object must have the hvr_table key and the associated value being the table hvr is replicating, the talend_job key and the associated talend cloud job to process the data from that table. If required a user can pass in parameters (context variables) to the talend job but this is optional.
The cli will create a checkpoint file that is used to let the cli know which hvr manifest file it processed successfully and to only process manifest files with a date newer. The HVR Manifest files are sorted by date so oldest to newest is processed. If the cli encounters an error it will print out a message and return a system code of 1 which lets the HVR system
know that an error has occurred. 
`
{
  "mappings": [
    {
        "hvr_table": "dm51_order",
        "talend_job": "hvr_demo"
    },
    {
      "hvr_table": "aaa",
      "talend_job": "job1",
      "parameters" : [
        {"name1":"value1"},
        {"name2": "value2"}
      ]
    }
  ]
}
`
### HVR Agent Plugin
In HVR we use the Agent Plugin to call our script. In this example we named the script `callTalendCloud.bat`

### License
MIT - <http://www.opensource.org/licenses/mit-license.php

