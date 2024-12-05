import { TextLineStream } from 'https://deno.land/std@0.208.0/streams/mod.ts';
import { stringify } from 'https://deno.land/std@0.199.0/csv/mod.ts';
import { ScopusClient } from './elsevier-clients/scopus-client.ts';
import { filter, flatMap, map, toArray } from './streams.ts';
import { ScopusSearchResponseBody } from './elsevier-types/scopus-types.ts';
import { ScopusAuthorSearchEntry } from './elsevier-types/scopus-author-search-types.ts';
import { ScopusAuthorSearchApi } from './elsevier-apis/scopus-author-search-api.ts';
import { ScopusSearchApi } from './elsevier-apis/scopus-search-api.ts';
import { ScopusSearchEntry } from './elsevier-types/scopus-search-types.ts';
import { readerToAsyncIterable } from './utils.ts';

const getFileName = (fileId: string) => {
  return `co-au-id-${fileId}.json` as const;
};

const getPairFileName = (fileId: string) => {
  return `pair-co-au-id-${fileId}.json` as const;
};

const apiKey = Deno.env.get('ELSEVIER_KEY');

if (!apiKey) {
  console.error(
    `API key must be specified by environment variable 'ELSEVIER_KEY'`,
  );
  Deno.exit(-1);
}

const apiKeys = apiKey.split(/\s*,\s*/gi).filter((value) => value !== '');

const configName = 'co-au-ai';
//const limit = 40;
//const sortedBy = '-document-count';

const inputFile = `./target/${configName}.txt` as const;
const outputDir = `./target/output/${configName}` as const;
const catchDir = outputDir;

const getCache = (fileId: string) => {
  return `${catchDir}/${getFileName(`${fileId}`)}` as const;
};

const getPairCache = (fileId: string) => {
  return `${catchDir}/${getPairFileName(`${fileId}`)}` as const;
};

const getOuput = (fileId: string) => {
  return `${outputDir}/${getFileName(`${fileId}`)}` as const;
};

const getPairOuput = (fileId: string) => {
  return `${outputDir}/${getPairFileName(`${fileId}`)}` as const;
};

await Deno.mkdir(outputDir, { recursive: true });

const client = new ScopusClient(apiKeys, 10);
const authorSearchApi = new ScopusAuthorSearchApi(client);
const scopusSearchApi = new ScopusSearchApi(client);

let count = 1;

//const idMap = new Map<string, string>();

const idMap = new Map<string, string>([
  ['7403147159', 'Wong,Tienyin'],
  ['11939875400', 'Yan,Shuicheng'],
  ['7004510847', 'Acharya,U. R.'],
  ['25924925100', 'Sam Ge,Shuzhi'],
  ['7003996538', 'Suganthan,Ponnuthurai Nagaratnam'],
  ['8919714700', 'Niyato,Dusit (Tao)'],
  ['35087158600', 'Xie,Liliua Hua'],
  ['24780933300', 'Lee,Tong Heng'],
  ['7101702977', 'Chua,Tat Seng'],
  ['56140547500', 'Cambria,Erik'],
  ['7201366998', 'Wen,Changyun'],
  ['8574872000', 'Lin,Weisi'],
  ['36439415700', 'Feng,Jiashi'],
  ['7102105011', 'Thakor,Nitish Vyomesh'],
  ['57221738247', 'Band,Shahab S.'],
  ['35611951900', 'Fujita,Hamido'],
  ['25522308800', 'Loy,Chen Change'],
  ['24473792300', 'Yuen,Chau'],
  ['7403425167', 'Sundararajan, N.'],
  ['35269388000', 'Lo,David'],
  ['7006684979', 'Srinivasan,Dipti'],
  ['55513410500', 'Xu,Jianxin'],
  ['14065346100', 'Lu,Jiwen'],
  ['54957302700', 'Wang,Gang'],
  ['8710996600', 'Hoi,Steven C.H.'],
  ['35231499000', 'Xu,Dong'],
  ['55362784200', 'Quek,Tony Q.S.'],
  ['57218294440', 'Gani,Abdullah'],
  ['7407077210', 'Wang,Danwei'],
  ['7006735298', 'Ong,Yew Soon'],
  ['7101632622', 'Guan,Cuntai'],
  ['36998895200', 'Pham,Viet Thanh'],
  ['24823334000', 'Yuan,Junsong'],
  ['56541062900', 'Heidari,Ali Asghar'],
  ['25655559700', 'Tan,Kay Chen'],
  ['7102842925', 'Er,Meng Joo'],
  ['35070838500', 'Zaidan,A. A.'],
  ['36601294900', 'Son,Lehoang'],
  ['8615868400', 'Li,Haizhou'],
  ['55328836300', 'Islam,Mohamad Tariqul'],
  ['7005885082', 'Thalmann,Daniel'],
  ['7006008493', 'Kendall,Graham'],
  ['15119385200', 'Tan,Chewlim'],
  ['55316592700', 'Poria,Soujanya'],
  ['7201984906', 'Tan,Ru San'],
  ['7003629165', 'Kankanhalli,Mohan S.'],
  ['55628524759', 'Brown,Michael Scott'],
  ['16068189400', 'Elshafie,Ahmed'],
  ['7401777066', 'Wen,Yonggang'],
  ['7403999687', 'Tan,Kok Kiong'],
  ['14030501000', 'Ren,Hongliang'],
  ['24762992500', 'Magnenat Thalmann,N.'],
  ['7201710034', 'Lim,Ee Peng'],
  ['35560724100', 'Wood,Kristin L.'],
  ['55726699700', 'Tan,Ping'],
  ['16178527100', 'Jiang,Xudong'],
  ['35792005500', 'Hu,Guoqiang'],
  ['15056385100', 'Kumam,Poom'],
  ['47061923200', 'Ng,Hwee Tou'],
  ['35317209500', 'Fu,Huazhu'],
  ['55739136600', 'Chen,Xudong'],
  ['55366935100', 'Zhang,Hanwang'],
  ['36439883200', 'Nie,Liqiang'],
  ['7006752756', 'Goh,Mark'],
  ['7402043226', 'Chen,I. Ming'],
  ['57188713282', 'Ni,Bingbing'],
  ['35588578100', 'Kot,Alex Chichung'],
  ['7408611448', 'Chen,Ben M.'],
  ['15751945400', 'Sundaram,Suresh'],
  ['7405853761', 'Yu,Haoyong'],
  ['7402047189', 'He,Bingsheng'],
  ['7402002763', 'Hsu,Wynne'],
  ['56066648800', 'Zhang,Yue'],
  ['7409117252', 'Lee,Mong Li'],
  ['7401711135', 'Cai,Wenjian'],
  ['8869387400', 'Quek,Hiok Chai'],
  ['7402139929', 'Tan,Yap-Peng'],
  ['8850060600', 'Miao,Chunyan'],
  ['7403153287', 'Cai,Jianfei'],
  ['35487931000', 'Li,Xiaoli'],
  ['7402802915', 'Fu,Chi Wing'],
  ['8439329200', 'Lu,Shijian'],
  ['7409079258', 'Li,Zhengguo'],
  ['23389932700', 'Jimmy,Liu Jiang'],
  ['56346558700', 'Zhang,Jie'],
  ['56911879800', 'Liu,Yang'],
  ['56881778600', 'Wang,Lipo'],
  ['7601491976', 'Wang,Youyi'],
  ['22939024800', 'Pan,Sinno'],
  ['7004407304', 'Sundararajan,N.'],
  ['55293196600', 'Pan,Yongping'],
  ['7404229052', 'Lin,Zhiping'],
  ['57208481391', 'Hussain,Aini'],
  ['34569904000', 'Hsu,David'],
  ['7004591575', 'Meriaudeau,Fabrice'],
  ['6603297760', 'Mat Isa,N. A.'],
  ['7101647298', 'Ang,Kai Keng'],
  ['56196028200', 'Jiang,Jing'],
  ['25921980900', 'Chen,C. H.'],
  ['55663394400', 'Lee,Wee Sun'],
  ['7006650541', 'Ang,Marcelo H.'],
  ['36445134800', 'Zhao,Peilin'],
  ['24724794600', 'Khader,Ahamad Tajudin'],
  ['7005440300', 'Bezerianos,Anastasios'],
  ['55378119700', 'Wei,Yunchao'],
  ['7202336776', 'Ong,Simheng'],
  ['23395783300', 'Abdullah,Salwani'],
  ['55423994500', 'Zimmermann,Roger'],
  ['56903519800', 'Wong,Damon Wk'],
  ['8897889300', 'Phan,Raphael C. W.'],
  ['16646213800', 'Kayacan,Erdal'],
  ['56152358600', 'Ji,Hui'],
  ['6507548054', 'Dauwels,Justin'],
  ['24474196000', 'Chng,Engsiong'],
  ['6602262501', 'Yaacob,Sazali Bin'],
  ['6701717388', 'Ser,Wee'],
  ['8945369100', 'Cheung,Nagai Man'],
  ['7005822439', 'Yau,Wei Yun'],
  ['7007112857', 'Cheah,C. C.'],
  ['7201882649', 'Tan,Ah Hwee'],
  ['12800348400', 'Malik,Aamir Saeed'],
  ['8948616300', 'Vasant,Pandian M.'],
  ['7402612617', 'Cyril,Leung'],
  ['16240990400', 'Ang,Wei Tech'],
  ['7004412916', 'Kassim,Ashraf A.'],
  ['55743334300', 'Zhao,Qi'],
  ['35147075900', 'Vo,Bay'],
  ['8901577100', 'An,Bo'],
  ['56428060700', 'Cheng,Li'],
  ['7403454337', 'Lim,Joo Hwee'],
  ['36975461300', 'He,Ying'],
  ['7102069451', 'Bi,Guoan'],
  ['15064023000', 'Chandrasekhar,Vijay Ramaseshan'],
  ['7003887302', 'Raveendran,Paramesaran'],
  ['55753004000', 'Yu,Han'],
  ['37036085800', 'Mostafa,Salama A.'],
  ['7403975657', 'Zheng,Jianmin'],
  ['35619439200', 'Shamsuddin,Siti Mariyam Hj'],
  ['26031818200', 'Yeow,Raye Chen Hua'],
  ['56437024900', 'Liu,Ziwei'],
  ['7202837226', 'Ma,Kaikuang'],
  ['57199116143', 'Lin,Guosheng'],
  ['35235920500', 'Zhong,Liang'],
  ['56153273100', 'Sun,Jun'],
  ['56335714100', 'Zhou,Joey Tianyi'],
  ['6602630793', 'Kerdcharoen,Teerakiat'],
  ['23393946900', 'Su,Rong'],
  ['57207799513', 'Pratama,Mahardhika'],
  ['37013313400', 'Lee,Gim Hee'],
  ['6603804395', 'Vinod,A. P.'],
  ['7006403609', 'Mao,Kezhi'],
  ['7102672302', 'Chau,Lap Pui'],
  ['7201984927', 'Tan,Robby T.'],
  ['57200684958', 'Lu,Jiangbo'],
  ['6602198919', 'Tangsrirat,Worapong'],
  ['53264815700', 'Sarno,Riyanarto'],
  ['55516961100', 'Xu,Yanwu'],
  ['36179737700', 'Jalab,Hamid A.'],
  ['57194450557', 'Chan,Chee Seng'],
  ['57201881045', 'Xiong,Zehui'],
  ['6508063649', 'Culaba,Alvin B.'],
  ['7102093739', 'Chui,Chee Kong'],
  ['8701576800', 'Zamli,K. Z.'],
  ['7403124759', 'Tang,Huajin'],
  ['6602077243', 'Prudente,Maricar Sison'],
  ['7005909381', 'Rajan,Deepu'],
  ['55663361300', 'Yeung,Sai Kit'],
  ['15623948900', 'Zhang,Haihong'],
  ['57206655762', 'Shen,Zhiqi'],
  ['55844477500', 'Zhang,Le'],
  ['24468984100', 'Selamat,Ali'],
  ['7407904780', 'Huang,Weimin'],
  ['36456051800', 'Elara Mohan,Rajesh'],
  ['35207032000', 'Sun,Yu'],
  ['56187964300', 'Dailey,Matthew N.'],
  ['57535555300', 'Cheng,Jun'],
  ['7004048032', 'Cheong,Loongfah'],
  ['7005189207', 'Sim,Terence'],
  ['6603819288', 'Seah,Hocksoon'],
  ['7201497272', 'Lau,Hoong Chuin'],
  ['24450495700', 'Haron,Habibollah'],
  ['7501444983', 'Li,Liyuan'],
  ['14833694300', 'Wong,Koksheik'],
  ['55663612200', 'Tee,Kengpeng'],
  ['57218701794', 'Gupta,Abhishek'],
  ['57200530694', 'Mustapha,Aida'],
  ['57194261987', 'Supriyanto,Eko'],
  ['33067448100', 'Caesarendra,Wahyu'],
  ['21833638600', 'Basu,Arindam'],
  ['56227831000', 'See,John'],
  ['16204410100', 'Wang,Han'],
  ['27267916200', 'Le,Bac Hoai'],
  ['36662390100', 'Long,Hoang Viet'],
  ['7003391055', 'Cham,Tatjen'],
  ['22938689300', 'Pham,Quang Cuong'],
  ['36088700700', 'Jhanjhi,N. Z.'],
  ['57210569784', 'Rahim,Mohd Shafry Mohd'],
  ['8433003800', 'Zhao,Shengdong'],
  ['15844435200', 'Wang,Chuanchu'],
  ['23497306400', 'Kong,Adams Wai Kin'],
  ['6507054880', 'Chen,Lihui'],
  ['56216710600', 'Taib,Mohd Nasir'],
  ['9942198800', 'Khalifa,Ohtman O.'],
  ['25652391600', 'Sriboonchiita,Songsak'],
  ['22235331000', 'Tan,U. Xuan'],
  ['57195741598', 'Yang,Jianfei'],
  ['57200208474', 'Setiadi,De Rosal Ignatius Moses'],
  ['57201906965', 'Chew,Chee Meng'],
  ['7102180182', 'Low,Bryan Kian Hsiang'],
  ['56036880900', 'Ahmad,Mohd Sharifuddin'],
  ['55753359600', 'Liu,Jun'],
  ['7401988807', 'Tang,Tong Boon'],
  ['8568432600', 'Jatmiko,Wisnu'],
  ['56698967300', 'Zhao,Jun'],
  ['55326454500', 'Ubando,Aristotle T.'],
  ['55663408900', 'Loo,Chu Kiong'],
  ['7401750566', 'Cai,Yiyu'],
  ['15622892900', 'Campolo,Domenico'],
  ['57218454758', 'So-In,Chakchai'],
  ['35617179000', 'Teoh,Earn Khwang'],
  ['56253605900', 'Kumngern,Montree'],
  ['7006508142', 'Yap,Kim Hui'],
  ['35085446500', 'Wong,Yongkang'],
  ['6602548473', 'Lauw,Hady Wirawan'],
  ['6602629924', 'Dadios,Elmer P.'],
  ['8620004100', 'Musirin,Ismail Bin'],
  ['23991445600', 'Shafie,S.'],
  ['35109046500', 'Faye,Ibrahima'],
  ['56567441400', 'Saad,Naufal'],
  ['55396044200', 'Lursinsap,Chidchanok'],
  ['57221053092', 'Teo,Kenneth'],
  ['6508137561', 'Goi,Bok Min'],
  ['56396097300', 'Wen,Bihan'],
  ['22635061700', 'Kamel,Nidal S.'],
  ['6507190996', 'Varakantham,Pradeep'],
  ['55843019200', 'Zhu,Hongyuan'],
  ['7003538204', 'Phua,Koksoon'],
  ['55925542700', 'Liu,Luoqi'],
  ['7402229700', 'Ng,Tian Tsong'],
  ['36809950300', 'Liu,Yisi'],
  ['6602604153', 'HeryPurnomo,Mauridhi'],
  ['57195728805', 'Abdul-Malek,Zulkurnain'],
  ['56168849900', 'Nooritawati,Md Tahir'],
  ['7102127975', 'Hassan,Rosilah'],
  ['55665262600', 'Ooi,Wei Tsang'],
  ['57189587485', 'Abdullah,Mohd Zaid Bin'],
  ['6602079797', 'Deris,Safaโ€Ai Bin'],
  ['35228592500', 'Abdullah,Mohd Lazim'],
  ['35310925800', 'Kartiwi,Mira'],
  ['7403322546', 'Nguyen,Hung T.'],
  ['57211550487', 'Xie,Lihua'],
  ['6603913023', 'Zainal,Anazida Binti'],
  ['57193848115', 'Sari,Christy Atika'],
  ['8454497000', 'Cheng,Xiang'],
  ['57193850466', 'Rachmawanto,Eko Hari'],
  ['8366154100', 'Sourin,Alexei I.'],
  ['26424426100', 'Tan,Ngan Meng'],
  ['57561111300', 'Savitha,Ramaswamy'],
  ['24724973200', 'Mohamad,Mohd Saberi'],
  ['8286407700', 'Gunawan,Teddy Surya'],
  ['24823900300', 'Hashim,Aisha Hassan Abdalla'],
  ['57218170038', 'Sahari,Khairul Salleh Mohamed'],
  ['8710428200', 'Zhang,Zhuo'],
  ['56004326900', 'Palaiahnakote,Shivakumara'],
  ['55268560900', 'Xie,Xiaofei'],
  ['24802568600', 'Mustapha,Norwati Binti'],
  ['56001754300', 'Soh,Harold S.H.'],
  ['35746873900', 'Prasad,Dilip K.'],
  ['57192165059', 'Thanh,Dang Ngoc Hoang'],
  ['24491506600', 'Mawardi,Bahri'],
  ['35176729700', 'Tran,Minh Triet'],
  ['57205093001', 'Hidayanto,Achmad Nizar'],
  ['55009925500', 'Pardamean,Bens'],
  ['55599317400', 'Bandala,Argel A.'],
  ['7005685730', 'Ibrahim,Zuwairie'],
  ['55938277000', 'Foong,Shaohui'],
  ['57202765861', 'Chamnongthai,Kosin'],
  ['57210591699', 'Nugroho,Hanung Adi'],
  ['57196217102', 'Theera-Umpon,Nipon'],
  ['24824003600', 'Asirvadam,V. S.'],
  ['55919537600', 'Li,Yu'],
  ['26632809300', 'Dennis,John Ojur'],
  ['7405549368', 'Zhou,Jiayin'],
  ['35226378800', 'Kusakunniran,Worapan'],
  ['57191896599', 'Zhao,Jian'],
  ['57200986655', 'Robiah,Ahmad'],
  ['56843751100', 'Suyanto,Suyanto'],
  ['6602825080', 'Ashari,Mochamad'],
  ['7102593407', 'Ibrahim,Rosdiazli Bin'],
  ['55666627500', 'Widyawan,Widyawan'],
  ['57217130776', 'Mat Darus,Intan Z.'],
  ['24448545300', 'Nallagownden,Perumal A.L.'],
  ['36988337700', 'Seera,Manjeevan'],
  ['57193858460', 'Tay,Kai Meng'],
  ['6602298043', 'Hadi Habaebi,Mohamed'],
  ['23009527800', 'Nanayakkara,Suranga Chandima'],
  ['36057249600', 'Abdullah,Siti Norul Huda Sheikh'],
  ['34267792700', 'Sheikh,Usman Ullah'],
  ['36338419400', 'Adiwijaya,Adiwijaya'],
  ['36608212500', 'Ibrahim,Roliana'],
  ['9942198000', 'Jeoti,Varun'],
  ['23397755200', 'Omar,N.'],
  ['55617604100', 'Lin,Jie'],
  ['26422482100', 'Rustam,Zuherman'],
  ['35611523900', 'Saad,N.'],
  ['57204666744', 'Syed Abu Bakar,Syed Abdul Rahman'],
  ['57207332880', 'Lin,Feng'],
  ['6505757323', 'Fanany,Mohamad Ivan'],
  ['57204345367', 'Sourina,Olga'],
  ['24312091400', 'Lim,K. B.'],
  ['55643637700', 'Sun,Qianru'],
  ['24829837100', 'Yin,Fengshou'],
  ['56336297600', 'Shuai,Bing'],
  ['36189141300', 'Su,Bolan'],
  ['57188769435', 'Li,Jianshu'],
  ['57200576499', 'Wan,W. K.'],
  ['25638780900', 'Munir,Achmad'],
  ['6506068255', 'Adiono,Trio'],
  ['43061470400', 'Shah,Asadullah'],
  ['57189235033', 'Meiryani,Meiryani'],
  ['16641940000', 'Binh,Huynh Thi Thanh'],
  ['16203945200', 'Supnithi,Pornchai'],
  ['7406500681', 'Yang,Xulei'],
  ['57219410727', 'Wan Hasan,Wan Zuha'],
  ['36069151100', 'Budiharto,Widodo'],
  ['7102334703', 'Sim,Kok Swee'],
  ['8666109300', 'Arshad,Mohd Rizal Bin'],
  ['36630482600', 'Md Khir,Mohd Haris'],
  ['57203294503', 'Nor,Nursyarizal Mohd'],
  ['7201930443', 'Osman,Muhammad Khusairi'],
  ['56346536100', 'Johan,Henry'],
  ['36815724000', 'Arymurthy,Aniati Murni'],
  ['16318609100', 'Ali,Syed Saad Azhar'],
  ['6602733151', 'Auephanwiriyakul,Sansanee'],
  ['23096031100', 'Soh,Gim Song'],
  ['35241970700', 'Ahmad,Tohari'],
  ['16549109100', 'Cahyadi,Adha Imam'],
  ['25825877400', 'Wahyunggoro,Oyas'],
  ['11840021200', 'Kusumoputro,Benyamin'],
  ['17436183800', 'Wongsawat,Yodchanan'],
  ['6603067787', 'Meesad,Phayung'],
  ['16203446200', 'Mandal,Bappaditya B.'],
  ['8563659400', 'Jin,Zhe'],
  ['7403366395', 'Tan,Shingchiang'],
  ['14025975500', 'Phuong,Tu Minti'],
  ['55445497800', 'Zick,Yair'],
  ['7404043105', 'Khan,Sheroz'],
  ['35796262100', 'Soeprijanto,Adi Adi'],
  ['22735122000', 'Mantoro,Teddy'],
  ['16641853800', 'Duong,Duc Anh'],
  ['7005028924', 'Sari,R. F.'],
  ['55218502200', 'Thalmann,Nadia Magnenat'],
  ['35811948800', 'Fatichah,Chastine'],
  ['23477809900', 'Le,Thi Lan'],
  ['14021131100', 'Budi,Indra'],
  ['36626798300', 'Zhang,Junmei'],
  ['24330191400', 'Suparta,Wayan'],
  ['24473788700', 'Soewito,Benfano'],
  ['55497208700', 'Sybingco,E.'],
  ['49663689900', 'Lubis,Muharman'],
  ['6506899735', 'Harjoko,Agus'],
  ['16175247200', 'Mansor,Wahidah'],
  ['26639610000', 'Nurmaini,Siti'],
  ['6602703083', 'Chongstitvatana,Prabhas'],
  ['56233862200', 'Rahiman,Mohd Hezri Fazalul'],
  ['57189522357', 'Ismail,I.'],
  ['6602725346', 'Widyantoro,Dwi Hendratmo'],
  ['55556605000', 'Hazim Alkawaz,Mohammed'],
  ['36607920500', 'Hua,Binh Son'],
  ['24492028400', 'Karim,Hezerul Abdul'],
  ['7404685895', 'Cheng,Shihfen'],
  ['55195057700', 'Nguyen,Loan T.T.'],
  ['55846541300', 'Prasetyo,Heri'],
  ['23103757500', 'Nguyen,Quang Uy'],
  ['23059348400', 'Anh,Ho Pham Huy'],
  ['55320094500', 'Van Hoa,Ngo'],
  ['57219696428', 'Warnars,Harco Leslie Hendric Spits'],
  ['57194337704', 'Zeng,Zeng'],
  ['56958616200', 'Sasmoko,Sasmoko'],
  ['24829867400', 'Vateekul,Peerapon'],
  ['57204827000', 'Prasetyo,Yogi Tri'],
  ['15759362600', 'Ab-Rahman,Mohammad Syuhaimi'],
  ['57202353784', 'Turnip,Arjon'],
  ['36807106400', "Syai'in,Mat"],
  ['56006507400', 'Theeramunkong,Thanaruk'],
  ['55666712800', 'Lam,Siew Kei'],
  ['8284838300', 'Su,Yi'],
  ['55396730800', 'Santoso,Harry B.'],
  ['17434540100', 'Joelianto,Endra'],
  ['24176993000', 'Mursanto,Petrus'],
  ['50263686000', 'Xiong,Wei'],
  ['26422337300', 'Priyadi,Ardyono'],
  ['15130697000', 'Chan,Jonathan H.'],
  ['55821741300', 'Ahmed,Syed Faiz'],
  ['24168509800', 'Hamid,Nor Hisham Bin'],
  ['57208041010', 'Concepcion,Ronnie'],
  ['57205093129', 'Suryanegara,Muhammad'],
  ['6507794445', 'Nugroho,Lukito Edi'],
  ['24825315200', 'Suciati,Nanik'],
  ['15064006500', 'Ahmad Fauzi,Mohammad Faizal'],
  ['55779190600', 'Mahmudy,Wayan Firdaus'],
  ['17436271100', 'Suthakorn,Jackrit'],
  ['23134907000', 'Li,Yiqun'],
  ['56600944500', 'Purwanto,Djoko'],
  ['9334481700', 'Drieberg,Micheal'],
  ['35108768700', 'Adnan,Ramli Bin'],
  ['56401363400', 'Wang,Di'],
  ['56596678600', 'Tjahjono,Anang'],
  ['7201762489', 'Janssen,Patrick H.T.'],
  ['55390963300', 'Ahmad,Azhana'],
  ['57220106535', 'Dela Cruz,Jennifer C.'],
  ['57196198202', 'Abu Bakar,Shahriman'],
  ['55178487200', 'Razlan,Zuradzman Mohamad'],
  ['55319953900', 'Vicerra,Ryan Rhay P.'],
  ['53264793000', 'Sensuse,Dana Indra'],
  ['46661988300', 'Sitompul,Opim Salim'],
  ['36662724200', 'Shapiai,M. I.'],
  ['13104011100', 'Purwarianti,Ayu'],
  ['57201737786', 'Setianingsih,Casi'],
  ['37013424100', 'Handayani,Putu Wuri'],
  ['35589381300', 'Ayu,Media A.'],
  ['36815737800', 'Bustamam,Alhadi'],
  ['55390566600', 'Suharjito,Suharjito'],
  ['24734043700', 'Adji,Teguh Bharata'],
  ['55820370500', 'Parasuraman,Subramanian'],
  ['55909526900', 'Ramli,Kalamullah'],
  ['56024813000', 'Tran,Thanh Hai'],
  ['56104446200', 'Vu,Hai'],
  ['36069000500', 'Ardiyanto,Igi'],
  ['55569020700', 'Al Rasyid, M. Udin Harun'],
  ['9636895500', 'Santosa,Paulus Insap'],
  ['24724121200', 'Erdt,Marius'],
  ['8987804700', 'Le,Dinhdinh'],
  ['13204394500', 'Pasupa,Kitsuchart'],
  ['8549374100', 'Visoottiviseth,Vasaka'],
  ['35811372900', 'Sulaiman,S. N.'],
  ['55556605000', 'Alkawaz,Mohammed Hazim'],
  ['24723916400', 'Hidayat,Risanuri'],
  ['57189996441', 'Sungkono,Kelly R.'],
  ['57193740909', 'Budiman,Edy'],
  ['55129691700', 'Tan,Min Keng'],
  ['56028467300', 'Sunat,Khamron'],
  ['57208210220', 'Lauguico,Sandy C.'],
  ['36940813800', 'Ngo,Long Thanh'],
  ['57217631746', 'Ibrahim,Zunaidi'],
  ['56321161400', 'Kannan,Ramani'],
  ['55834784000', 'Suhartono,Derwin'],
  ['37079357500', 'Kadir,Kushsairy'],
  ['57194571217', 'Trilaksono,Bambang Riyanto'],
  ['6602656099', 'Rahardjo,Eko Tjipto'],
  ['35100882700', 'Sukaridhoto,Sritrusta'],
  ['7003593814', 'Mohd Noor,Norliza'],
  ['57201216710', 'Haviluddin,Haviluddin'],
  ['57216658927', 'Isa,S. M.'],
  ['57215833019', 'Setiawan,Noor Akhmad'],
  ['35794232900', 'Purnama,I. Ketut Eddy'],
  ['48761265400', 'Farzamnia,Ali'],
  ['35093069600', 'Samad,Abd Manan'],
  ['56716397400', 'Purwandari,Betty'],
  ['26654457700', 'Wibirama,Sunu'],
  ['55488741100', 'Utaminingrum,Fitri'],
  ['25825391000', 'Permanasari,Adhistya Erna'],
  ['25626881300', 'Widyotriatmo,Augie'],
  ['57015628300', 'Yamaka,Woraphon'],
  ['53264185600', 'Khodra,Masayu Leylia'],
  ['21742658500', 'Hussin,Fawnizu Azmadi'],
  ['57190940136', 'Wibowo,Antoni'],
  ['55847263000', 'Rivai,Muhammad'],
  ['45961439100', 'Sudarsono,Amang'],
  ['6602373768', 'Gunawan,Dadang'],
  ['23493551500', 'Pujiantara,Margo'],
  ['55567473300', 'Liu,Jiangxu'],
  ['36701624700', "Mohd Su'ud,Mazliham"],
  ['57189096030', 'Bedruz,Rhen Anjerome R.'],
  ['7405440850', 'Lee,Benghai'],
  ['57188875498', 'Lubis,Arif Ridho'],
  ['57015379700', 'Maneejuk,Paravee'],
  ['6507998737', 'Phimoltares,Suphakant'],
  ['8921529200', 'Widyanto,Muhammad Rahmat'],
  ['55387633100', 'Chin,Renee Ka Yin'],
  ['57211339497', 'Alejandrino,Jonnel Dorado'],
  ['36554491300', 'Lim,Kian Ming'],
  ['13610410800', 'Song,Insu'],
  ['36519082900', 'Lee,Chin Poo'],
  ['6603566678', 'Thammano,Arit'],
  ['24536664300', 'Gaol,Ford Lumban'],
  ['56012410400', 'Wibawa,Aji Prasetya'],
  ['57211128612', 'Nguyen,Vinh Tiep'],
  ['25032210400', 'Supnithi,Thepchai'],
  ['55194163800', 'Young,Michael Nayat'],
  ['6506896570', 'Supangkat,Suhono Harso'],
  ['6506147709', 'Hariadi,Mochamad'],
  ['56820169100', 'Abdurachman,Edi'],
  ['57203859161', 'Meyliana,null'],
  ['57205972690', 'Wibowo,A.'],
  ['57198792998', 'Medina,Ruji P.'],
  ['8353843300', 'Nababan,Erna Budhiarti'],
  ['36806716400', 'Penangsang,Ontoseno'],
  ['55932829400', 'Alamsyah,Andry'],
  ['9944012900', 'Heryadi,Yaya'],
  ['36778184500', 'Arifin,Agus Zainal'],
  ['8935281500', 'Gerardo,Bobby D.'],
  ['35175282300', 'Prasetyo,Eri W.'],
  ['56119401500', 'Sarwinda,Devvi'],
  ['24726221500', 'Wibawa,Adhi Dharma'],
  ['35280403200', 'Mardiyanto,Ronny'],
  ['36806091600', 'Gan Lim,Laurence A.'],
  ['6601998412', 'Lukas,L.'],
  ['36350907600', 'Isa,Iza Sazanita Binti'],
  ['6507613469', 'Surendro,Kridanto'],
  ['35759156200', 'Nilsook,Prachyanun'],
  ['23974402000', 'Ketcham,Mahasak'],
  ['56596841200', 'Anggriawan,Dimas Okky'],
  ['6507756188', 'Kerdprasop,Nittaya'],
  ['6506154091', 'Kerdprasop,Kittisak'],
  ['57201132764', 'Vo,Chau Thi Ngoc'],
  ['55953473400', 'Tian,Shuangxuan'],
  ['54894437700', 'Ballado,Alejandro'],
  ['56741519000', 'Baldovino,Renann G.'],
  ['8057050300', 'Setijadi Prihatmanto,Ary'],
  ['27067738900', 'Huynh,Hiep Xuan'],
  ['24172624800', 'Paglinawan,Arnold C.'],
  ['8594464600', 'Promwong,Sathaporn'],
  ['6504093131', 'Riyadi,Munawar A.'],
  ['6508292922', 'Pramadihanto,Dadet'],
  ['42462707300', 'Sumpeno,Surya'],
  ['53664570200', 'Surjandari,Isti'],
  ['7409884994', 'Muljono,Muljono'],
  ['56204479000', 'Djamal,Esmeralda Contessa'],
  ['36185944100', 'Mustika,I. Wayan'],
  ['36841697700', 'Ngo,Thanhduc'],
  ['6507465058', 'Sucahyo,Yudho Giri'],
  ['23494175000', 'Suryoatmojo,Heri'],
  ['24476646200', 'Rochimah,Siti'],
  ['56502202000', 'Rosmansyah,Yusep'],
  ['57200657978', 'Tung Ngo,Son'],
  ['55625531300', 'Wibisono,Ari'],
  ['56619583100', 'Machbub,Carmadi'],
  ['55233408900', 'Yulita,Intan Nurma'],
  ['55120215600', 'Madenda,Sarifuddin'],
  ['57204513246', 'DIng,Henghui'],
  ['9133666000', 'Kistijantoro,Achmad Imam'],
  ['35849297000', 'Oranova Siahaan,Daniel'],
  ['57203922071', 'Hasibuan,Zainal A.'],
  ['54407677800', 'Sison,Ariel M.'],
  ['57200036923', 'Nguyen,Thach Ngoc'],
  ['35305743100', 'Risnumawan,Anhar'],
  ['8932217500', 'Sulistijono,Indra Adji'],
  ['6507688023', 'Vatanawood,Wiwat'],
  ['24766148400', 'Hung,Phan Duy'],
  ['57201739056', 'Irawan,Budhi'],
  ['7403431845', 'Shen,Shengmei'],
  ['55926240900', 'Murfi,Hendri'],
  ['55334395300', 'Syarif,Iwan'],
  ['35758895900', 'Hernandez,Alexander A.'],
  ['55458137900', 'Ahmad,Adang S.'],
  ['25646368600', 'Nurhadi,Hendro'],
  ['9334901600', 'Basaruddin,T.'],
  ['16641924000', 'Anh,Duong Tuan'],
  ['53264206800', 'Kristalina,Prima'],
  ['15726583000', 'Tajjudin,Mazidah'],
  ['55972482700', 'Chaveesuk,Singha'],
  ['57189238414', 'Yuliana,Mike'],
  ['56491378300', 'Chaiyasoonthorn,Wornchanok'],
  ['14021772900', 'Subagdja,Budhitama'],
  ['24172720300', 'Cruz,Febus Reidj G.'],
  ['57196421450', 'Tolentino,Lean Karlo'],
  ['57194163567', 'Caya,Meo Vincent C.'],
  ['56968173800', 'Yuniarno,Eko Mulyanto'],
  ['6506923289', 'Nawawi,Zainuddin'],
  ['57199228691', 'Purnomo,Agung'],
  ['57222487822', 'Prabowo,Harjanto'],
  ['55835973000', 'Abbas,Bahtiar Saleh'],
  ['55320560100', 'Setijadi,Eko'],
  ['36337949500', 'Trisetyarso,Agung'],
  ['35811196100', 'Sigit,Riyanto'],
  ['55668171000', 'Shidik,Guruh Fajar'],
  ['57210368825', 'Lai-Kuan,Wong'],
  ['56771397100', 'Billones,Robert Kerwin C.'],
  ['57189056872', 'Bautista Linsangan,Noel'],
  ['56771148800', 'Valenzuela,Ira'],
  ['57193666780', 'Murad,Dina Fitria'],
  ['24734366700', 'Murti,Muhammad Ary'],
  ['55161949900', 'Aziz,Azrina Abd'],
  ['57194546265', 'Surjandy,Surjandy'],
  ['16641907100', 'Dinh,Dien'],
  ['49663102500', 'Ferdiana,Ridi'],
  ['36194762800', 'Kosala,Raymond'],
  ['35119619500', 'Yahya,Norashikin'],
  ['48361474500', 'Kusumaningrum,Retno'],
  ['56626063000', 'Pinem,Ave Adriana'],
  ['27367627700', 'Barakbah,Ali Ridho'],
  ['57194160083', 'Azzahro,Fatimah'],
  ['53863823400', 'Benny Mutiara,Achmad'],
  ['16423632300', 'Suhartanto,Heru'],
  ['24778445000', 'Riawan,Dedet Candra'],
  ['16550792300', 'Sardjono,Tri Arief'],
  ['6701505994', 'Setiawan,Agung Wahyu'],
  ['57217065427', 'Ratna,Anak Agung Putri'],
  ['57216270748', 'Sang,Dinh Viet'],
  ['35092754100', 'Janin,Zuriati'],
  ['55489397100', 'Herumurti,Darlis'],
  ['56126778500', 'Wibowo,Wahyu Catur'],
  ['36197845900', 'Ahmed,Falah Younis H.'],
  ['39961309200', 'Fillone,Alexis M.'],
  ['35106220200', 'Sulistyo,Selo'],
  ['57189058237', 'Sari,Yuita Arum'],
  ['35786064200', 'Lestari,Dessi Puji'],
  ['53264745200', 'Setiabudy,Rudy'],
  ['56811538300', 'Chumuang,Narumol'],
  ['22333564000', 'Dejdumrong,Natasha'],
  ['56479735300', 'Cui,Jian'],
  ['55120354700', 'Do,Phuc Thinh'],
  ['35975312300', 'Suwadi,undefined'],
  ['38761832800', 'Ong,Simying'],
  ['56480052000', 'Lan,Zirui'],
  ['54917468100', 'Festijo,Enrique D.'],
  ['36626689700', 'Huynh,Quyet Thang'],
  ['56363073800', 'Shihab,Muhammad Rifki'],
  ['57195431920', 'Flores Villaverde,Jocelyn'],
  ['56278679300', 'Dela Cruz,Angelo R.'],
  ['6506871127', 'Sidik,Muhammad Abu Bakar'],
  ['54392364700', 'Asfani,Dimas Anton'],
  ['56119154900', 'Ruldeviyani,Yova'],
  ['57189355900', 'Fernando,Erick'],
  ['55016662200', 'Utama,Ditdit Nugeraha'],
  ['57219208769', 'Made Yulistya Negara,I.'],
  ['57201078267', 'Ulkhaq,M. Mujiya'],
  ['57188877288', 'Fajar,Ahmad Nurul'],
  ['36056199500', 'Bugtai,Nilo T.'],
  ['35145405200', 'Pham,Bao T.'],
  ['36057015500', 'Kusrini,Kusrini'],
  ['25824690200', 'Bandung,Yoanes'],
  ['56904198800', 'Domingo-Palaoag,Thelma'],
  ['55441028000', 'Susiki Nugroho,Supeno Mardi'],
  ['36678103500', 'Setyohadi,Djoko Budiyanto'],
  ['36668537900', 'Hashimoto,Kiyota'],
  ['35796613800', 'Utami,Ema'],
  ['57190762441', 'Karnjana,Jessada'],
  ['36471197400', 'Saputro,Adhi Harmoko'],
  ['18133646400', 'Abdurohman,Maman'],
  ['53264382000', 'Mutijarsa,Kusprasapta'],
  ['36809779800', 'Kusumawardani,Sri Suning'],
  ['56010497200', 'Madyatmadja,Evaristus Didik'],
  ['56039019600', 'Dewantara,Bima Sena Bayu'],
  ['6507249299', 'Netinant,Paniti'],
  ['43061747600', 'So,Rosa Qui Yue'],
  ['13205413800', 'Arifin,Achmad'],
  ['6506715346', 'Poespawati,Nji Raden'],
  ['35102111900', 'Harsono,Tri'],
  ['57193712876', 'Hidayat,Egi'],
  ['57203803755', 'Gumilar,Langlang'],
  ['6508122813', 'Chinnasarn,Krisana'],
  ['55014363300', 'Bachtiar,Fitra Abdurrachman'],
  ['55821184400', 'Hnoohom,Narit'],
  ['35759613600', 'Wannapiroon,Panita'],
  ['36659741000', 'Purnamasari,Prima Dewi'],
  ['6505807438', 'Budiardjo,Eko Kuswardono'],
  ['57202941004', 'Fajardo,Arnel C.'],
  ['56126918100', 'Lim,Kit Guan'],
  ['57204952401', 'Nashiruddin,Muhammad Imam'],
  ['6508036628', 'Rasmequan,Suwanna'],
  ['16647429100', 'Rodtook,Annupan'],
  ['36023393000', 'Amphawan,Komate'],
  ['36344067500', 'Samonte,Mary Jane C.'],
  ['57189060174', 'Pacis,Michael C.'],
  ['57188667562', 'Roxas,Edison A.'],
  ['54384447800', 'Yumang,Analyn Niere'],
  ['55598790600', 'Soeleman,Moch Arief'],
  ['54787407700', 'Apriono,Catur'],
  ['6507312291', 'Jambak,Muhammad Irfan'],
  ['49663289500', 'Durachman,Yusuf'],
  ['55836286200', 'Suzianti,Amalia'],
  ['56521579100', 'Budi,Nur Fitriah Ayuning'],
  ['23995884800', 'Solano,Geoffrey A.'],
  ['56537681800', 'Nguyen,Khang M.T.T.'],
  ['57188708917', 'Handoko,Bambang Leo'],
  ['55479283600', 'Wong,Chee Onn'],
  ['35796376900', 'Suprapto,Yoyon Kusnendar'],
  ['57193725995', 'Pham,Van Hau'],
  ['57189267265', 'Noprisson,Handrie'],
  ['6602473011', 'Affandi,Achmad'],
  ['35301229500', 'Yow,Aiping'],
  ['36999949200', 'Kurniawan,Arief'],
  ['6506958605', 'Mitrpanont,Jarernsri L.'],
  ['56770725300', 'Jose,John Anthony C.'],
  ['35096014900', 'Bejo,Agus'],
  ['55948804900', 'Ly,Ngocquoc'],
  ['7006070850', 'Hadi,Setiawan'],
  ['57193578866', 'Balbin,Jessie R.'],
  ['56770534000', 'Garcia,Ramon G.'],
  ['57193645353', 'Magwili,Engr Glenn V.'],
  ['57214836304', 'Wang,Gunawan'],
  ['56049242500', 'Oktavia,Tanty'],
  ['57193701633', 'Handayani,Anik Nur'],
  ['57193895504', 'Basuki,Achmad'],
  ['56441525100', 'Fahmi,Daniar'],
  ['57194548460', 'Sardjono,Wahyu'],
  ['7202124724', 'Caro,Jaime D.L.'],
  ['55746722900', 'Nurtanio,Ingrid'],
  ['56045311900', 'Lystianingrum,Vita'],
  ['55369837900', 'Ardi,Romadhani'],
  ['57189090952', 'Ketut Eddy Purnama,I. K.E.'],
  ['57192980983', 'Rachmadi,Reza Fuad'],
  ['56989738400', 'Ningrum,Endah Suryawati'],
  ['16041627800', 'Alasiry,Ali Husein'],
  ['57191244271', 'Putrada,Aji Gautama'],
  ['57188979780', 'Gandhi,Arfive'],
  ['57188651943', 'Tjhin,Viany Utami'],
  ['6504452433', 'Bachtiar,Mochamad Mobed'],
  ['57212566384', 'Nguyen,Hoangphuong'],
  ['26635681800', 'Pratomo,Istas'],
  ['36625306900', 'Do,Thuan P.'],
  ['56105039600', 'Sirisrisakulchai,Jirakom'],
  ['56118930500', 'Suryani,Mira'],
  ['57201076074', 'Wati,Masna'],
  ['57202197381', 'Sunyoto,Andi'],
  ['35731392400', 'Endah,Sukmawati Nur'],
  ['57188574161', 'Hariadi,Farkhad Ihsan'],
  ['55645367600', 'Musa,Purnawarman'],
  ['22734711500', 'Pipanmaekaporn,Luepol'],
  ['24604923500', 'Kamonsantiroj,Suwatchai'],
  ['56577742600', 'Manuel,Mark Christian E.'],
  ['57191851850', 'Ranti,Benny'],
  ['57208230337', 'Gumelar,Agustinus Bimo'],
  ['57723908000', 'Martinez,Jesus M.'],
  ['57201649104', 'Lagman,Ace'],
  ['57205446141', 'Fanani,Ahmad Zainul'],
  ['49961277000', 'Ermatita,Ermatita'],
  ['55630996700', 'Muslim,Erlinda'],
  ['55598224500', 'Gerasta,Olga Joy L.'],
  ['57205119837', 'Raharjo,Teguh'],
  ['57202304004', 'Munsayac,Francisco Emmanuel T.'],
  ['6506955711', 'Chawakitchareon,Petchporn'],
  ['57189233423', 'Alam,Basuki Rachmatul'],
  ['55625410200', 'Rakun,Erdefi'],
  ['57208326489', 'Blancaflor,Eric B.'],
  ['57189059979', 'Valiente,Flordeliza'],
  ['57200075336', 'Dimaunahan,Ericson D.'],
  ['57195411225', 'Qomariyah,Nunung Nurul'],
  ['50661996800', 'Sulthoni,Muhammad Amin'],
  ['56582470800', 'Lindawati,Ang Swat Lin'],
  ['56798594800', 'Echevarria Aguja,Socorro'],
  ['57204035025', 'Atmojo,Idam Ragil Widianto'],
  ['57204648016', 'Castillo,Reynaldo E.'],
  ['57209778050', 'Tolentino,Roselito E.'],
  ['57208204149', 'Verdadero,Marvin S.'],
  ['56165484200', 'Pane,Murty Magda M.M.P.'],
  ['56179751600', 'Ardiansyah,Roy'],
  ['57216852891', 'Sholeh,Mokhammad'],
  ['57196711177', 'Siregar,Christian C.S.'],
  ['57220030069', 'Thi van Pham,Anh'],
  ['6503849022', 'Elamvazuthi,I.'],
] as (readonly [string, string])[]);

const [cachingCoauthorStream, previousCoauthorStream] = ReadableStream.from(
  readerToAsyncIterable(await Deno.open(inputFile, { read: true })),
)
  .pipeThrough(new TextDecoderStream())
  .pipeThrough(new TextLineStream())
  .pipeThrough(filter((value) => value.trim() !== ''))
  .pipeThrough(map((value) => value.split('\t').map((term) => term.trim())))
  .pipeThrough(
    map(async ([id, name, ind]) => {
      //idMap.set(id, name);
      const index = `${count++}`.padStart(5, ' ');
      console.info(`start [${index}]:`, id, name, ind);
      // const query = `AU-ID(${id}) AND FIRSTAUTH(${name})`;
      const query = `AU-ID(${id})`;
      console.info(`\t[${index}] loading: ${query}`);
      try {
        if (id === '') {
          throw new Error(`The scopus-id for "${name}" is empty`);
        }

        let isCached = true;

        const body = await (async () => {
          try {
            return JSON.parse(
              await Deno.readTextFile(getCache(id)),
            ) as ScopusSearchResponseBody<ScopusAuthorSearchEntry>;
          } catch {
            isCached = false;
            return await authorSearchApi.search(
              {
                'co-author': id,
                view: 'STANDARD',
                // sort: sortedBy,
                count: 1,
                field: 'url',
              },
              (limit, remaining, reset, status) =>
                console.info(
                  `\t[${index}] rateLimit: ${remaining?.padStart(
                    5,
                    ' ',
                  )}/${limit} reset: ${reset} [${status}]`,
                ),
            );
          }
        })();

        console.info(
          `\t[${index}] loaded`,
          `${body['search-results']['entry'].length}/${body['search-results']['opensearch:totalResults']}`,
        );
        return { index, id, name, ind, isCached, body };
      } catch (err: unknown) {
        console.error(`\t[${index}] error`, err);
        return {
          index,
          id,
          name,
          ind,
          isCached: false,
          body:
            err instanceof Error
              ? new Error(err.message, {
                  cause: {
                    query: query,
                    error: err.cause,
                    origin: err,
                  },
                })
              : new Error(`${err}`, {
                  cause: {
                    query: query,
                    origin: err,
                  },
                }),
        };
      }
    }),
  )
  .tee();

const coAuthorRegEx = /au\-id\((\d+)\)/gi;

const coauthorStream = previousCoauthorStream
  .pipeThrough(
    filter(
      (
        data,
      ): data is typeof data & {
        body: ScopusSearchResponseBody<ScopusAuthorSearchEntry>;
      } => !(data.body instanceof Error),
    ),
  )
  .pipeThrough(
    flatMap(({ index, id, name, ind, body }) =>
      ReadableStream.from(
        [
          ...body['search-results']['opensearch:Query'][
            '@searchTerms'
          ].matchAll(coAuthorRegEx),
        ]
          .map((row) => row[1])
          .filter((coId) => coId !== id)
          .map((coId) => ({
            index,
            id,
            name,
            ind,
            coId,
          })),
      ),
    ),
  );

const cachingCoauthorPromise = cachingCoauthorStream.pipeTo(
  new WritableStream({
    async write({ index, id, name, isCached, body }) {
      if (isCached && getCache(id) === getOuput(id)) {
        console.info(`\t[${index}] done: cached`);
        return;
      }

      const isError = body instanceof Error;
      const encoder = new TextEncoder();
      const dataArray = encoder.encode(
        JSON.stringify(
          isError
            ? {
                name: body.name,
                message: body.message,
                cause: body.cause,
              }
            : body,
          undefined,
          2,
        ),
      );

      console.info(`\t[${index}] writing to file`);
      const indexPrefix = `inx${index.replaceAll(' ', '0')}`;
      const fileId = id === '' ? `${indexPrefix}-${name}` : id;
      const prefix = isError ? `error-${indexPrefix}-` : '';
      const fpOut = await Deno.open(getOuput(`${prefix}${fileId}`), {
        create: true,
        write: true,
      });
      await fpOut.write(dataArray);
      fpOut.close();
      console.info(`\t[${index}] done`);
    },
  }),
);

function getPairValue(coauthor1: string, coauthor2: string): string {
  if (coauthor1 < coauthor2) {
    return `${coauthor1}-${coauthor2}`;
  } else {
    return `${coauthor2}-${coauthor1}`;
  }
}

const pairCoauthorSet = new Set<string>();

const [cachingPairCoauthorStream, previousPairCoauthorStream] =
  ReadableStream.from(await toArray(coauthorStream))
    .pipeThrough(
      filter(({ id, coId }) => {
        if (idMap.has(coId)) {
          return false;
        }

        const pariValue = getPairValue(id, coId);
        if (pairCoauthorSet.has(pariValue)) {
          return false;
        }

        pairCoauthorSet.add(pariValue);
        return true;
      }),
    )
    .pipeThrough(
      map((data, index) => ({
        ...data,
        index: `${index++}`.padStart(5, ' '),
        pariValue: getPairValue(data.id, data.coId),
      })),
    )
    .pipeThrough(
      map(async ({ index, id, name, ind, coId, pariValue }) => {
        console.info(`start [${index}]:`, id, name, ind);

        const query = `AU-ID(${id}) AND AU-ID(${coId})`;
        console.info(`\t[${index}] loading: ${query}`);
        try {
          let isCached = true;

          const body = await (async () => {
            try {
              return JSON.parse(
                await Deno.readTextFile(getPairFileName(pariValue)),
              ) as ScopusSearchResponseBody<ScopusSearchEntry>;
            } catch {
              isCached = false;
              return await scopusSearchApi.search(
                {
                  query,
                  view: 'STANDARD',
                  // sort: sortedBy,
                  count: 1,
                  field: 'url',
                },
                (limit, remaining, reset, status) =>
                  console.info(
                    `\t[${index}] rateLimit: ${remaining?.padStart(
                      5,
                      ' ',
                    )}/${limit} reset: ${reset} [${status}]`,
                  ),
              );
            }
          })();

          console.info(
            `\t[${index}] loaded`,
            `${body['search-results']['entry'].length}/${body['search-results']['opensearch:totalResults']}`,
          );
          return {
            index,
            id,
            name,
            ind,
            coId,
            pariValue,
            isCached,
            body,
            type: 'result' as const,
          };
        } catch (err: unknown) {
          console.error(`\t[${index}] error`, err);
          return {
            index,
            id,
            name,
            ind,
            coId,
            pariValue,
            isCached: false,
            body:
              err instanceof Error
                ? new Error(err.message, {
                    cause: {
                      query: query,
                      error: err.cause,
                      origin: err,
                    },
                  })
                : new Error(`${err}`, {
                    cause: {
                      query: query,
                      origin: err,
                    },
                  }),
            type: 'error' as const,
          };
        }
      }),
    )
    .tee();

const pairCoauthorStream = previousPairCoauthorStream
  .pipeThrough(
    filter(
      (data): data is Exclude<typeof data, { type: 'error' }> =>
        data.type !== 'error',
    ),
  )
  .pipeThrough(
    map(({ index, id, ind, coId, pariValue, body }) => {
      return {
        index: index,
        'au-id-1': id,
        'au-data-name-1': idMap.get(id),
        'au-id-2': coId,
        'au-data-name-2': idMap.get(coId),
        'pair-value': pariValue,
        ind: ind,
        'co-document-count': body['search-results']['opensearch:totalResults'],
      };
    }),
  );

const cachingPairCoauthorPromise = cachingPairCoauthorStream.pipeTo(
  new WritableStream({
    async write({ index, id, pariValue, isCached, body }) {
      if (isCached && getPairCache(id) === getPairOuput(id)) {
        console.info(`\t[${index}] done: cached`);
        return;
      }

      const isError = body instanceof Error;
      const encoder = new TextEncoder();
      const dataArray = encoder.encode(
        JSON.stringify(
          isError
            ? {
                name: body.name,
                message: body.message,
                cause: body.cause,
              }
            : body,
          undefined,
          2,
        ),
      );

      console.info(`\t[${index}] writing to file`);
      const indexPrefix = `inx${index.replaceAll(' ', '0')}`;
      const fileId = pariValue;
      const prefix = isError ? `error-${indexPrefix}-` : '';
      const fpOut = await Deno.open(getPairOuput(`${prefix}${fileId}`), {
        create: true,
        write: true,
      });
      await fpOut.write(dataArray);
      fpOut.close();
      console.info(`\t[${index}] done`);
    },
  }),
);

const writeResultPromise = (async () => {
  const fp = await Deno.open(`${outputDir}/result.csv`, {
    create: true,
    write: true,
    truncate: true,
  });

  let columns: string[] | null = null;

  await pairCoauthorStream
    .pipeThrough(
      flatMap((entry) => {
        if (columns === null) {
          columns = Object.keys(entry);

          return ReadableStream.from([
            stringify([columns], { headers: false }),
            stringify([entry], { headers: false, columns }),
          ]);
        }

        return ReadableStream.from([
          stringify([entry], { headers: false, columns }),
        ]);
      }),
    )
    .pipeThrough(new TextEncoderStream())
    .pipeTo(fp.writable);
})();

await Promise.all([
  cachingCoauthorPromise,
  cachingPairCoauthorPromise,
  writeResultPromise,
]);
