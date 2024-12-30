%macro gen_SRC_in_CHAR(orig_src=,orig_delim=,sas_tblname=,realtime_meta=, encode=) ;
/*
	%let t_rptmth=201504;
	%let txtfile=RMP2BIO5B;
	%let pmeta=raw_rmp.meta_rmp_reimsb_initial;
	%let orig_src=&dir_rmp./&t_rptmth./&txtfile._043.txt;
	%let orig_delim="|";
	%let sas_tblname=stg_rmp.&txtfile._&t_rptmth.;
	%let realtime_meta=work.rtmeta_&txtfile._&t_rptmth.;
	%let encode=ywin; 
*/
/*
%let orig_src=&dir_bbr./&folder./&file.;
%let orig_delim=",";
%let sas_tblname=stg_bbr.bbr_output_&folder.;
%let realtime_meta=work.rtmeta_bbr_output_&folder.;

*/
	options obs=1;
	proc import 
		datafile="&orig_src."
		out=work._srcfieldname 
		dbms=dlm replace;
		delimiter=&orig_delim.; 
		getnames=no;
	 	datarow=1;
	run;
	options obs=max;
	proc contents data=work._srcfieldname out=_srcfieldname_content noprint;run;
	proc transpose data=work._srcfieldname out=work._srcfieldname_tx;
		var var:;
	run;
	data work._srcfieldname_tx1;
		set work._srcfieldname_tx;
		where strip(COL1) not in ('' '.');
		length machine_name $255;
		machine_name=tranwrd(substr(strip(COL1),1,min(length(COL1),32)),"'", "''"); /* check and single quote in the fieldname, apply adjustment if exist. */
		_ERROR_=0;
		realtime_seq=_n_;
	run;
	proc sort data=work._srcfieldname_tx1;
		by machine_name realtime_seq;
	run;
	data work._srcfieldname_tx2;
		set work._srcfieldname_tx1 (rename=(col1=orig_name));
		by machine_name realtime_seq;
		if first.machine_name * last.machine_name ne 1 then do; /* there exist dup fieldnames after 32 chars-trimmed. */
			if first.machine_name ne 1 then do; 
				tmp=min(length(strip(machine_name)),32-6);
				machine_name=substr(strip(machine_name),1,tmp)||"_"||strip(put(realtime_seq,z5.)); /* handle duplicated field names */
			end;
		end;
		drop tmp;
	run;
	proc sort data=work._srcfieldname_tx2 out=&realtime_meta.;
		by realtime_seq; 
	run;
	%macro runtime_ds_syntax(src=, dat=, type=, var=);
		data _null_;
			file "&dir_tmp./&dat." lrecl=65535;
			length tmp $512; 
			set &src. end=eof;
			if _n_=1 then do; put "&type. "; end;
		%if &type. ne label %then %do;
			tmp = "'"||strip(machine_name)||"'n"; 
		%end;
		%else %do;
			tmp = "'"||strip(machine_name)||"'n  = '"||tranwrd(strip(orig_name),"'", "''")||"'"; 
		%end;
			put "0920"x tmp " &var."; 
			if eof then do;	put ";"; end;
		run;
	%mend;
/*	%runtime_ds_syntax(src=&realtime_meta., dat=tmp1&st_TS..dat, type=length, var=$255);*/
	%runtime_ds_syntax(src=&realtime_meta., dat=tmp1&st_TS..dat, type=length, var=$4000);
	%runtime_ds_syntax(src=&realtime_meta., dat=tmp2&st_TS..dat, type=input, var=$);
	%runtime_ds_syntax(src=&realtime_meta., dat=tmp3&st_TS..dat, type=label, var=);

	data &sas_tblname.;
		infile "&orig_src."
		delimiter=&orig_delim.
		dsd
		lrecl=65535
		truncover
		recfm=v
		firstobs=2
		%if &encode. ne %then %do;
			/*encoding='ywin'*/
			encoding="&encode."
		%end;
		;
		%include "&dir_tmp./tmp1&st_TS..dat" /source2; 
		%include "&dir_tmp./tmp2&st_TS..dat" /source2; 
		%include "&dir_tmp./tmp3&st_TS..dat" /source2; 
	run;

	/* Correct the exceeded space in variable */
	/* http://www.pharmasug.org/proceedings/2012/CC/PharmaSUG-2012-CC17.pdf*/
	proc format;
		invalue charlen
		0<-2   = 5
		2<-5   = 10
		5<-10  = 20
		10<-20 = 40
		20<-40 = 80
		40<-80 = 150
		80<-160 = 255
		160<-512= 1000
		512<-high= 2000
		;
	run;
	data _null_;
		file "&dir_tmp./charlen&st_TS..dat" lrecl=65535;
		set &sas_tblname. (obs=1);
		array vvv {*} _all_;
		put "proc sql noprint;";
		put "  create table cntfieldvar as ";
		put "  select ";
		do i = 1 to dim(vvv);
			tmp = "'"||tranwrd(vname(vvv(i)),"'","''")||"'n";
			put "  max(length(" tmp ")) as " tmp;
			if i ne dim(vvv) then put "  ,";
		end;
		put "  from &sas_tblname. ;";
		put "quit;";
	run;
	%include "&dir_tmp./charlen&st_TS..dat" /source2; 

	data _null_;
		file "&dir_tmp./charlen2&st_TS..dat";
		set cntfieldvar;
		array vvv {*} _all_;
		put "proc sql noprint; ";
		put "  alter table &sas_tblname. modify ";
		do i = 1 to dim(vvv);
			tmp = "'"||tranwrd(vname(vvv(i)),"'","''")||"'n";
			_tlen = input(vvv(i),charlen.); 
			put tmp "  char(" _tlen ")";
			if i ne dim(vvv) then put "  ,";
		end;
		put "  ;";
		put "quit;";
	run;
	%include "&dir_tmp./charlen2&st_TS..dat" /source2; 

%mend;
/*
%gen_SRC_IN_CHAR(orig_src=&dir_bpe./RSME003_csv/RSME003_043_201507_201512.csv,			orig_delim=",",	sas_tblname=stg.bpe_rsme003_201507_201512,		realtime_meta=work.rtmeta_bpe_rsme003_201507_201512,		encode=); 
%gen_SRC_IN_CHAR(orig_src=&dir_rmp./201606/RMP2BIO2_043.txt, 							orig_delim="|", sas_tblname=stg_rmp.RMP2BIO2_201606, 			realtime_meta=work.rtmeta_RMP2BIO2_201606, encode=ywin); 
		
*/
