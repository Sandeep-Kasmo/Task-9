import pandas as pd #type:ignore
from extract import *
from transform import *
from load import *


def main():

    #establish connection
    establish_connection()

    #load tables
    customer_df=get_data('select * from customer')
    order_df=get_data('select * from orders')
    new_data_df=get_data('select * from new_data')

    #print dataframes
    if customer_df is not None:
        print(customer_df)
    if order_df is not None:
        print(order_df)
    
    if new_data_df is not None:
        print(new_data_df)

    # perform transformations
    transformed_customer=customer_transform(customer_df)
    print(f'Transformed_customer:\n{customer_df}\n')
    new_customer_transform=customer_transform(new_data_df)
    print(f'New customers transformed data:\n{new_customer_transform}\n')

    # now apply scd type-2
    try:
        scd_type_2=SCDType2(transformed_customer,new_customer_transform)
        print(scd_type_2)
    except Error as e:
        print(f"Error performing scd type 2:{e}")
        return None
    
    # now create and insert data into sql
    try:
        load_data(scd_type_2,'scd_type_2')
    except Error as e:
        print(f"Error:{e}")
    
    




    #close connection
    close_connection()

if __name__=='__main__':
    main()