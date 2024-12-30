/* ************************************************************************************************ */
/* To import the source file in SAS dataset in characters format for avoiding information loss.		*/
/* The SAS dataset in characters format is saved in libname "STG", we will have regular				*/
/* housekeeping on every 2 years in this libname "STG".												*/
/* ************************************************************************************************ */

%macro gen_initial_CAR(meta=);
%if &flag_initial. = YES %then %do;
	/* %let meta=CAR_CorpExp; */
	filename carlist pipe "ls -l &dir_car./Corp/csv|grep -i '.csv'|grep -i '&meta.'"; * where directory is the path to your library;
	data carcsv_fileinfo;
		infile carlist;
		input;
		csvname = strip(scan(_infile_,9," "));
		c_ymd = scan(scan(csvname ,1,"."),-1,"_");
		c_ym = substr(strip(c_ymd),1,6);
		d_ym = intnx("MONTH",input(c_ymd,yymmdd8.),0,"END");
	run;
	%import_InputMaster(sheet=MetaInfo_&meta.,	out=raw_car.meta_&meta._initial);
	proc sql noprint; select count(1) into :cnt from carcsv_fileinfo_v1; quit;
	%do tmpcnt = 1 %to &cnt.;
	/*%do tmpcnt = 1 %to 1;*/
		data _null_;
			set carcsv_fileinfo_v1(firstobs=&tmpcnt. obs=&tmpcnt.);
			call symput("csvname",strip(csvname));
			call symput("t_ym",strip(c_ym));
			call symput("t_ymd",strip(c_ymd));
		run;
		/* %let csvname=CAR_CorpExp_20130930.csv; */
		x "cd &dir_car./Corp/csv";
		x "file -bi &dir_car./Corp/csv/&csvname. > chkencode.txt";
		data encode_info;
			infile "&dir_car./Corp/csv/chkencode.txt" 
			DLM='7F'x MISSOVER DSD ;
			INPUT  text   : $CHAR100.;
			encode = reverse(substr(reverse(strip(text)),1,5));
			if encode ="utf-8" then do;
				call symput("encode","");
			end;
			else do;
				call symput("encode","ywin");	
			end;
		run;

		%gen_SRC_IN_CHAR(orig_src=&dir_car./Corp/csv/&meta._&t_ymd..csv, orig_delim=",", sas_tblname=stg_car.&meta._&t_ym., realtime_meta=work.rtmeta_&meta._&t_ym., encode=&encode.); 
		%trim_SRC_FIELDNAME(proposed_meta=raw_car.meta_&meta._initial, realtime_meta=work.rtmeta_&meta._&t_ym., final_meta=stg_car.rtmeta_&meta._&t_ym.);
		%update_META2SRC(metatbl=stg_car.rtmeta_&meta._&t_ym., srctbl=stg_car.&meta._&t_ym., tartbl=raw_car.&meta._&t_ym.);
	%end;
%end;
%mend;

%exportlog(ind=START, path=&dir_log., name=CAR_INIT_LOAD_&st_cltinfo.);
%import_Format(src=CAR);
%gen_initial_CAR(meta=CAR_CorpExp);
%gen_initial_CAR(meta=CAR_BankFIExp);
%gen_initial_CAR(meta=CAR_ExRating);
%exportlog(ind=STOP);
