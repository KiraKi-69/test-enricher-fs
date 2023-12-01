import logging
import os
import requests
import json


logger = logging.getLogger(__name__)

class FeastRestEnricher(object):
    
    def __init__(self):
        import os

        self.url = os.environ['feast_feature_server_url']
        
        
    def transform_input(self, X, features_names):
        logger.debug("Enriching features: %s and values %s", features_names, X)
        
        zipcode = X[features_names.index('zipcode')]
        dob_ssn = X[features_names.index('dob_ssn')]

        logger.debug("Get data: zipcode %s and dob_ssn %s", zipcode, dob_ssn)
        
        req = {
            "feature_service": "loan_features",
            "entities": {
                "zipcode": [int(zipcode)],
                "dob_ssn": [dob_ssn]
            }
        }

        logger.debug("Send request to server: %s", json.dumps(req))
        
        response = requests.post(
            f"{self.url}get-online-features",
            data=json.dumps(req)
        )
        
        feature_vector = {}
        
        if response.status_code == 200:
            response_dict = response.json()
            logger.debug("response dict: %s", response_dict)
            for idx, column in enumerate(response_dict['metadata']['feature_names']):
                feature_vector[column] = response_dict['results'][idx]['values']
        
            for name in features_names:
                if feature_vector.get(name) is None:
                    if name in ["loan_amnt", "person_age"]:
                        feature_vector[name] = int(X[features_names.index(name)])
                    elif name in ["person_emp_length", "loan_int_rate"]:
                        feature_vector[name] = float(X[features_names.index(name)])
                    else:
                        feature_vector[name] = X[features_names.index(name)]
        
        logger.debug("result vector: %s", feature_vector)
        
        return feature_vector