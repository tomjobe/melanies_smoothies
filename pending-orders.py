# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write("""Orders that need to be filled""")

cnx = st.connection("snowflake")
session = cnx.session()
my_dataframe = session.table("smoothies.public.orders").filter(col("order_filled")==0).collect()
#st.dataframe(data=my_dataframe, use_container_width=True)

if my_dataframe:
    editable_df = st.data_editor(my_dataframe)
    #edited_df = st.data_editor(my_dataframe, key="ORDER_ID",num_rows="fixed",column_order=("ORDER_FILLED","ORDER_ID","NAME_ON_ORDER","INGREDIENTS"))

    submitted = st.button('Submit')

    if submitted:
 
        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(editable_df)
    
        try:
            og_dataset.merge(edited_dataset
                     , (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
                     , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                    )
            st.success('Order Completed', icon = '👍')

        except:
            st.write("Somthing has gone wrong!")

else:
    st.success("No pending orders !" ,icon = '👍')
  
