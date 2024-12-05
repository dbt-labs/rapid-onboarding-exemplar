version: 2

sources:
  - name: stripe2
    description: Incoming stripe payment data.
    database: raw
    schema: stripe
    tables:
      - name: payment
        columns:
          - name: id
            data_tests:
              - not_null
              - unique
          - name: paymentmethod
            data_tests:
              - accepted_values:
                  values: ['credit_card', 'bank_transfer', 'gift_card', 'coupon']
          - name: status
            data_tests:
              - accepted_values:
                  values: ['success', 'fail']