# the intent of this .md is to remove redundancy in the documentation

# the below are descriptions from order_items
{% docs base_price %} since extended_price is the line item total, we back out the price per item {% enddocs %}

{% docs discounted_price %} factoring in the discount_percentage, the line item discount total {% enddocs %}

{% docs tax_rate %} tax rate of the order item {% enddocs %}

{% docs gross_item_sales_amount %} same as extended_price {% enddocs %}

{% docs discounted_item_sales_amount %} line item (includes quantity) discount amount{% enddocs %}

{% docs item_discount_amount %} item level discount amount. this is always a negative number {% enddocs %}

{% docs item_tax_amount %} item level tax total {% enddocs %}

{% docs net_item_sales_amount %} the net total which factors in discount and tax {% enddocs %}