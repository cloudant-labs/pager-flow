pager-flow
==========

This application performs the following tasks:

* Retrieves incident data through PagerDuty API.
* Constructs a JSON object for each incident.
* Uploads each incident as a document to Cloudant database.

Configuration
---

Fill out the example.settings.ini file to your settings and save it under the same directory as pagerflow.py.

How to Run
---

Execute the following command:

    $ python pagerflow.py example.settings.ini
    
Other Notes
---

* "unresolved" view must list all of the incidents with a status of "unresolved" from the database.
* The application will upload ALL incidents if: 
      - It is an initial execution
      - Log file is invalid / empty
      - Log file does not exist under the same directory
