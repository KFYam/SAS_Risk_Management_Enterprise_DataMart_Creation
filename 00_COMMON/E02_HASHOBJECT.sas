%macro hash_apply(data_in=, key=, data=, hash_ds=, output_ds=, init_vars=, update_logic=, lookup_logic=, missing_value=Missing);
   data &output_ds;
      length &init_vars;
      set &data_in;

      if _n_ = 1 then do;
         /* Declare and initialize hash object */
         declare hash h_obj (dataset: "&hash_ds");
         h_obj.definekey("&key");
         h_obj.definedata(&data);
         h_obj.definedone();
      end;

      /* Lookup logic */
      &lookup_logic;

      /* Handle missing values if any */
      if &data = '' then &data = "&missing_value";

      /* Custom update logic (if needed) */
      &update_logic;
   run;
%mend;



%hash_apply(
   data_in=xmpl.customer_mail,
   key=source,
   data=description,
   hash_ds=xmpl.source_code,
   output_ds=xmpl.mailing_w_descriptions,
   init_vars=description $35,
   update_logic=,
   lookup_logic=if h_obj.find() ge 0;
);

