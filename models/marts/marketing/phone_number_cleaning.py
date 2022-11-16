import phonenumbers

def format_phone_number(phone_number, country_code):
        # parse the phone number with the ISO country code
        parsed = phonenumbers.parse(phone_number, country_code.strip())

        # format the phone number with the correct phone country code
        formatted = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.INTERNATIONAL)

        return formatted



    

def model(dbt, session):
    dbt.config(
        materialized="table",
        packages = ["phonenumbers"]
    )

    customers = dbt.ref("dim_customers").to_pandas()

    customers["PHONE_NUMBER_FORMATTED"] = customers.apply(
                                                            lambda x: format_phone_number(
                                                                x.PHONE_NUMBER,
                                                                x.ISO_CODE
                                                            ),
                                                            axis=1 
                                                        )


    return customers