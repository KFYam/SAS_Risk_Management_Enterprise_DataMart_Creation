%macro mergeDelta(key=, src=, delta=, out=, flag_ind=);
	proc sort data=&src. out=tmp; by &key.; run;
	data &out.;
		merge tmp(in=a) &delta.(in=b);
		by &key.;
		if a;
		%if &flag_ind. ne %then %do;
		if b then &flag_ind.&key.=1;
		%end;
	run;
%mend;

%macro getCARExRating(key=, src=, out=);
	/* %let key=_id2; %let out=work.ex_rating_id2; */
	proc sort data=&src.(keep=&key. ex_rating) out=&out.(rename=(
			ex_rating=ex_rating&key.
			%if &key.=_id2 %then %do; _id2=_id %end;
		))
		nodupkey; 
		where not missing(&key.) and not missing(ex_rating);
		by &key.; 
	run;
%mend;

%macro gen_RMPBankFI(stdte=, rmp=, out=);
	/*  
	%let rmp=rmp2biobank; 
	%let stdte=201503;	
	%let out=rmp2biobank; 
	*/
	%let tmp_yymm	= &stdte.;
	%let RMPfile 	= raw_rmp.&rmp.A_&tmp_yymm.;
	%let Outfile 	= mart.&out._&tmp_yymm.;
	%let dte		= %SYSFUNC(intnx(MONTH,%SYSFUNC(inputn(&tmp_yymm.01,yymmdd8.)),0,END));


	/* ************************************************************************************************* */
	/* STEP 1: Count the no of records from source file (Fullset) as at Observation (Backward) YrMth */
	/* ************************************************************************************************* */
	proc sql noprint; select count(1) into: osrc_cnt from &RMPfile.; quit;
	%put [==> Sense Checking <==]: ORIGINAL SOURCE COUNT is &osrc_cnt.;

	/* ************************************************************************************************* */
	/* STEP 1.1: Generate mapping key between union of (HO_COR_CODE, COR_CODE), ID and CIN */
	/* ************************************************************************************************* */
	proc sort data=raw_car.car_BankFIExp_&tmp_yymm.(keep=ho_cor_bk entity_code)
		out=work.mapping_hocorbk(rename=(ho_cor_bk=_id_bk)) nodupkey;
		by ho_cor_bk entity_code;
		where not missing(ho_cor_bk);
	run;
	proc sort data=raw_car.car_BankFIExp_&tmp_yymm.(keep=cor_bk entity_code)
		out=work.mapping_corbk(rename=(cor_bk=_id_bk)) nodupkey;
		by cor_bk entity_code;
		where not missing(cor_bk);
	run;
	proc sort data=raw_car.car_BankFIExp_&tmp_yymm.
		out=work.mapping_id(keep=id entity_code rename=(id=_id_bk)) nodupkey;
		by id entity_code;
		where missing(cor_bk) and missing(ho_cor_bk) and not missing(id);
	run;
	data mapping_id_bk(rename=(entity_code=_cin));
		length sasflag_bkcodesrc $20;
		set work.mapping_hocorbk(in=a) work.mapping_corbk(in=b) work.mapping_id(in=c);
		if a then sasflag_bkcodesrc="ho_cor_bk"; *headoffice code;
		if b then sasflag_bkcodesrc="cor_bk"; *corp code;
		if c then sasflag_bkcodesrc="id"; *id;
		if a or b then _id_bk=catt("5",_id_bk);
		entity_code=strip(entity_code);
	run;
	proc sort data=work.mapping_id_bk out=work.mapping_id_bk_v1; 
		by _id_bk _cin sasflag_bkcodesrc;
	run;
	proc sort data=work.mapping_id_bk_v1 out=stg_oth.&rmp._idcinkey_&tmp_yymm. nodupkey; 
		by _id_bk _cin;
	run;
	/* ************************************************************************************************* */
	/* STEP 1.2: Based on the mapping key to get the EXPOSURE from exposure files */
	/* ************************************************************************************************* */
	/* Note: merge cannot be used in this case because many cin to many cins join in this case */
	/* where the unique keys of expsoure table are entity_code, contract_type */
	/* where the unique keys of mapping table are cin , id */
	data tmp_car_BankFIExp;
		length orig_seq 8 region $3;
		set raw_car.car_BankFIExp_&tmp_yymm.;
		orig_seq = _n_;
		entity_code=strip(entity_code);
		region=substr(entity_code,1,3);
	run;
	proc sql noprint;
		create table stg_oth.&rmp._keyexp_&tmp_yymm. as
		select a._id_bk, a.sasflag_bkcodesrc, b.* 
		from stg_oth.&rmp._idcinkey_&tmp_yymm. a left join tmp_car_BankFIExp b
		on a._cin = strip(b.entity_code)
		order by a._id_bk, b.region, a.sasflag_bkcodesrc, b.orig_seq;
	quit;
	/* ************************************************************************************************* */
	/* STEP 1.3: summ up the exposure by id_bk and region level */
	/* ************************************************************************************************* */
	proc sql noprint;
		create table tbl_ead_all as
			select _id_bk, sum(ead) as ead_all, sum(ead_pre_ccf) as ead_pre_ccf_all
			from stg_oth.&rmp._keyexp_&tmp_yymm. 
			group by _id_bk order by _id_bk;
		create table tbl_ead_hkg as
			select _id_bk, sum(ead) as ead_043, sum(ead_pre_ccf) as ead_pre_ccf_043
			from stg_oth.&rmp._keyexp_&tmp_yymm. 
			where region="HKG"
			group by _id_bk order by _id_bk;
		create table tbl_ead_chn as
			select _id_bk, sum(ead) as ead_243, sum(ead_pre_ccf) as ead_pre_ccf_243
			from stg_oth.&rmp._keyexp_&tmp_yymm. 
			where region="CHN"
			group by _id_bk order by _id_bk;
	quit;
	data work.tbl_ead_combined_idlevel(rename=(ead_all=ead ead_pre_ccf_all=ead_pre_ccf));
		merge tbl_ead_all (in=a) tbl_ead_hkg(in=b) tbl_ead_chn(in=c);
		by _id_bk;
		if a;
	run;
	/* ************************************************************************************************* */
	/* STEP 2.0: Identify intra bank group */
	/* ************************************************************************************************* */
	%update_ManualAdj(table_name=MART_RMP, src=&RMPfile., tar=work.tmp_rmp_base0, st_date=&stdte.);

	/* ************************************************************************************************* */
	/* STEP 2.1:  EADs' information in RMP file */
	/* ************************************************************************************************* */
	data work.tmp_rmp_base;
		set work.tmp_rmp_base0;
		length _cin $40 _id_bk $40;
		if missing(_cin) and not missing(n_mainland_cin) 		then _cin = catt("CHN-",put(n_mainland_cin,z11.));
		if missing(_cin) and not missing(n_hk_cin) 				then _cin = catt("HKG-",put(n_hk_cin,z11.));
		if not missing(c_id_type) or not missing(c_id_number)	then _id_bk=catt(c_id_type,c_id_number);
		rmp_orig_seq = _N_;
	run;
	proc sort data=work.tmp_rmp_base; by _id_bk; run;
	data work.RMP_EAD(rename=(_id_bk=_id));
		merge work.tmp_rmp_base(in=a) work.tbl_ead_combined_idlevel(in=b);
		by _id_bk;
		if a;
	run;
	/* ************************************************************************************************* */
	/* STEP 3.1: Retrieve the CAR External Rating */
	/* ************************************************************************************************* */
	data work.car_exrating;
		set raw_car.car_exrating_&tmp_yymm.;
		length _cin _id _id2 $40 ex_rating $40;
		if missing(_id2) and not missing(id_number_2) and substr(strip(id_number_1),1,1) = "5" then _id2 = catt("5",id_number_2); *Special handle the BANK Head Office Code;
		if missing(_id2) and not missing(id_number_2) 	then _id2 = strip(id_number_2); *Note: bank code should not be added as prefix of id, otherwise, rating cannot be assigned on the same id with different regions ;
		if missing(_id) and not missing(id_number_1) 	then _id = strip(id_number_1);
		if missing(_cin) and not missing(entity_code) 	then _cin = strip(entity_code);

		if missing(ex_rating) and not missing(sp_long_term_rating) 				then ex_rating = strip(sp_long_term_rating);
		/*if missing(ex_rating) and not missing(sp_long_term_rating_local_ccy) 	then ex_rating = strip(sp_long_term_rating_local_ccy);*/
		keep _cin _id _id2 ex_rating;
	run;

	/* ************************************************************************************************* */
	/* STEP 3.2: Retrieve external ratings by CIN and ID levels respectively */
	/* ************************************************************************************************* */
	%getCAREXRating(key=_id,	src=work.car_exrating, out=work.car_exrating_id);
	%getCAREXRating(key=_id2,	src=work.car_exrating, out=work.car_exrating_id2);
	%getCAREXRating(key=_cin,	src=work.car_exrating, out=work.car_exrating_cin);

	/* ************************************************************************************************* */
	/* STEP 4.1: Merge all information of Ex Rating information by ID, ID2 and CIN levels */
	/* ************************************************************************************************* */
	%mergeDelta(key=_id,	src=work.RMP_EAD,			delta=work.car_exrating_id,  out=work.RMP_EAD_EXRAT1);
	%mergeDelta(key=_id,	src=work.RMP_EAD_EXRAT1,	delta=work.car_exrating_id2, out=work.RMP_EAD_EXRAT2);
	%mergeDelta(key=_cin,	src=work.RMP_EAD_EXRAT2,	delta=work.car_exrating_cin, out=work.RMP_EAD_EXRAT3);

	/* ************************************************************************************************* */
	/* STEP 4.2: Join external ratings From S&P Source and Bloomberg rating */
	/* ************************************************************************************************* */
	proc sql noprint;
		create table work.RMP_EAD_EXRAT4 as 
			select a.*, b.sp_issuer_lt_fc_rating as ex_rating_sp, b.sp_entity_id
			from work.RMP_EAD_EXRAT3 a left join mart.sp_rating_&stdte.(where=(missing(sasflag_noinfo))) b
			on strip(a.c_id_type)=strip(b.RMP_id_type) and strip(a.c_id_number)=strip(b.RMP_id_number)
		;
		create table work.RMP_EAD_EXRAT5 as
			select a.*, b.Issuser_LT_FC_Rating as ex_rating_bb, b.Bloomberg_Ticker as bb_ticker
			from work.RMP_EAD_EXRAT4 a left join raw_oth.bloomberg_rating (where=(effect_dt_from <= &dte. <= effect_dt_to)) b
			on strip(a.c_id_type)=strip(b.RMP_id_type) and strip(a.c_id_number)=strip(b.RMP_id_number)
		;
	quit;

	/* ************************************************************************************************* */
	/* STEP 4.2: Define ultimate external rating */
	/* ************************************************************************************************* */
	data work.RMP_EAD_EXRAT6(rename=(_id=_id_bk));
		set RMP_EAD_EXRAT5;
		length ex_rating $40 ex_rating_src $3;
		if missing(ex_rating) and not missing(ex_rating_sp) then do; ex_rating = ex_rating_sp; ex_rating_src='sp'; end;
		if missing(ex_rating) and not missing(ex_rating_bb) then do; ex_rating = ex_rating_bb; ex_rating_src='bb'; end;
		if missing(ex_rating) and not missing(ex_rating_id) then do; ex_rating = ex_rating_id; ex_rating_src='id'; end;
		if missing(ex_rating) and not missing(ex_rating_id2) then do; ex_rating = ex_rating_id2; ex_rating_src='id2'; end;
		if missing(ex_rating) and not missing(ex_rating_cin) then do; ex_rating = ex_rating_cin; ex_rating_src='cin'; end;
		if missing(ex_rating) then ex_rating = '<Missing>';
	run;

	/* ************************************************************************************************* */
	/* STEP 5.1: Merge the 1st default information after the observation month */
	/* ************************************************************************************************* */
	/* Since there is no bank code in RMP Bank FI while DDM id has bank code prefix, we need to "create" */
	/* CHN id and HKG id as the same time for checking default status */
	data work.RMP_EAD_EXRAT_DDM0;
		length _cin $255 _id_hkg $255 _id_chn $255;
		set work.RMP_EAD_EXRAT6;
		_id_hkg=catx("_","HKG",_id_bk);
		_id_chn=catx("_","CHN",_id_bk);
	run;
	%macro getDDM(key=, src=, out=, region=);
		%if %sysfunc(exist(mart.DDM&key._AFTER_&tmp_yymm.)) %then %do;
			proc sort data=mart.DDM&key._AFTER_&tmp_yymm.(keep=
				d_1st_default_start_date c_all_default_type key&key.
				)  
				out=tmpddm(rename=(
					key&key.=&key.&region.
					d_1st_default_start_date	=d_1st_default_start_date&key.&region.
					c_all_default_type			=c_all_default_type&key.&region.
				))
				nodupkey; 
				by key&key.; 
			run;
			%mergeDelta(key=&key.&region., src=&src., delta=tmpddm, out=&out., flag_ind=sasflag_ddm);
		%end;
		%else %do;
			data &out.; set &src.; run;
		%end;	
	%mend;
	%getDDM(key=_cin, src=work.RMP_EAD_EXRAT_DDM0, 	out=work.RMP_EAD_EXRAT_DDM1);
	%getDDM(key=_id,  src=work.RMP_EAD_EXRAT_DDM1,	out=work.RMP_EAD_EXRAT_DDM2, region=_hkg );
	%getDDM(key=_id,  src=work.RMP_EAD_EXRAT_DDM2,	out=work.RMP_EAD_EXRAT_DDM3, region=_chn );

	data &Outfile.;
		set work.RMP_EAD_EXRAT_DDM3;
		%if %sysfunc(exist(mart.DDM_CIN_AFTER_&tmp_yymm.)) or %sysfunc(exist(mart.DDM_ID_AFTER_&tmp_yymm.))  %then %do;
		length sasflag_ddm_cin sasflag_ddm_id_hkg sasflag_ddm_id_chn sasflag_ddm_ind 8;
		sasflag_ddm_ind = max(sasflag_ddm_cin, sasflag_ddm_id_hkg, sasflag_ddm_id_chn );
		%end;
	run;
%mend;
/* ************************************************************************************************* 
%gen_RMPBankFI(stdte=&st_BackMth.,	rmp=RMP2BIOBANK,	out=RMP2BIOBANK_ALL);  	*Bank model - full set;
%gen_RMPBankFI(stdte=&st_RptMth.,	rmp=RMP2BIOBANK,	out=RMP2BIOBANK_ALL);  	*Bank model - full set;
*/
