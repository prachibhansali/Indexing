package com.ir.indexing.tokenizer;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ir.indexing.stemmer.Stemmer;
public class QueryParser {

	static HashMap<String,Integer> terms = new HashMap<String,Integer>();
	static HashMap<Integer,String> t = new HashMap<Integer,String>();
	static HashMap<Integer,String> docs = new HashMap<Integer,String>();

	static String docroot = "/Users/prachibhansali/Documents/IR/Assignment2/Index-StemmerStopwords/";
	public static void main(String[] args) throws Exception {
		performParsing();
		
		/*RandomAccessFile f = new RandomAccessFile(docroot+"index.txt","rw");
		HashMap<String,HashMap<Integer,Float>> tfindex =  new HashMap<String,HashMap<Integer,Float>>();
		HashMap<Integer, Integer> doclengths = new HashMap<Integer,Integer>();
		loadFromFile(terms,docs,t);

		Catalog c = new Catalog();
		c.loadFromFile(docroot+"catalog.txt");
		ArrayList<Integer> qterms = new ArrayList<Integer>();
		qterms.add(3178);
		qterms.add(125);
		qterms.add(10231);
		SkipGramMinSpanForDoc(20,c,f,qterms);*/
	}

	private static void performParsing() throws Exception {
		RandomAccessFile f = new RandomAccessFile(docroot+"index.txt","rw");
		HashMap<String,HashMap<Integer,Float>> tfindex =  new HashMap<String,HashMap<Integer,Float>>();
		HashMap<Integer, Integer> doclengths = new HashMap<Integer,Integer>();

		Catalog c = new Catalog();
		c.loadFromFile(docroot+"catalog.txt");
		loadFromFile(terms,docs,t);

		int totaldocs = docs.size();

		HashMap<Integer,Integer> docFreq = new HashMap<Integer,Integer>();
		double avglength = (fetchDocFreqOfAllTerms(f,c,docFreq,doclengths,t))/totaldocs;
		System.out.println("********"+avglength);
		int maxlength = Collections.max(doclengths.values());
		// Fetch Vocabulary Count of corpus
		long vocabcount = c.size();

		// Fetching stopwords
		HashMap<String,Integer> stopwords = new HashMap<String,Integer>();
		extractStopwords(stopwords);
		
		ParseQuery(f,c,docFreq,stopwords,avglength,doclengths,t,totaldocs,vocabcount,maxlength);

	}

	private static void ParseQuery(RandomAccessFile f, Catalog c,
			HashMap<Integer, Integer> docFreq,
			HashMap<String, Integer> stopwords, double avglength,
			HashMap<Integer, Integer> doclengths, HashMap<Integer, String> t, int totaldocs,long vocabsize, int maxlength) throws NumberFormatException, IOException {

		//Start Query Parsing 
		HashMap<Integer,HashMap<Integer,Integer>> index =  new HashMap<Integer,HashMap<Integer,Integer>>();
		HashMap<Integer,ArrayList<Integer>> queryKeywords = new  HashMap<Integer,ArrayList<Integer>>();
		ArrayList<Integer> queryId = new ArrayList<Integer>();
		float bcons=0.75f, k1=1.2f, k2=2;
		File file = new File("/Users/prachibhansali/Documents/workspace/ElasticSearch/AP_DATA/query_desc.51-100.short.txt");
		//File file = new File("/Users/prachibhansali/Documents/workspace/ElasticSearch/AP_DATA/query.txt");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String query;

		// Parse through all the queries
		while((query = br.readLine())!=null && !query.trim().equals(""))
		{
			List<Integer> qterms = new LinkedList<Integer>();
			query = removeSpecialChars(query);

			String [] allterms = query.split("-|\\s+");

			ArrayList<String> alterms = new ArrayList<String>();
			for(int k=0;k<allterms.length;k++)
				alterms.add(allterms[k]);	

			int Qnum = Integer.parseInt(allterms[0]);
			queryId.add(Qnum);
			System.out.println("Running for query " + Qnum);

			for(int i=4; i<alterms.size(); i++) {
				Stemmer s = new Stemmer();
				char w[] = alterms.get(i).toLowerCase().toCharArray();
				s.add(w, w.length);
				s.stem();

				//String s = allterms[i].toLowerCase().trim();
				if(!stopwords.containsKey(alterms.get(i).toLowerCase().trim()))
				{
					String term = s.toString();
					System.out.print("terms == " + term+" ");
					if(!terms.containsKey(term)) continue;
					int id = terms.get(term);
					if(!index.containsKey(id))
					{
						//System.out.println("***************** Fetching TF for " + term +"******************");
						index.put(id,retrievePostings(f,c,id,docFreq));
						//print(index.get(id));
						qterms.add(id);					
					}
				}
			}
			queryKeywords.put(Qnum, new ArrayList<Integer> (qterms));

		}

		// Fetching total term freqs for every query term
		System.out.println("Start calculating ttfs");
		HashMap<Integer,Integer> TTF = new HashMap<Integer,Integer>();
		Iterator<Integer> terms_itr = index.keySet().iterator();
		while(terms_itr.hasNext())
		{
			int term = (int) terms_itr.next();
			Iterator doc_itr = index.get(term).keySet().iterator();
			int count = 0;
			while(doc_itr.hasNext()){
				int docname = (int) doc_itr.next();
				count+=index.get(term).get(docname);
			}
			TTF.put(term, count);
		}
		System.out.println("Done calculating ttfs");


		for(int j=0; j<queryId.size();j++)
		{
			HashMap<Integer, HashMap<Integer, Double>> okapi= new HashMap<Integer, HashMap<Integer, Double>>();
			computeOkapiValues(okapi,getDistinct(queryKeywords.get(queryId.get(j))),index,avglength,queryId.get(j).intValue(),doclengths);
			HashMap<Integer, Double> okapi_ranks = computeForOkapiModel(queryId.get(j),okapi);
			/*ArrayList<String> tfidf = computeFortfidfModel(queryId.get(j),okapi,totaldocs,docFreq);
			ArrayList<String> okapibm_ranks = computeForokapiBM25(queryId.get(j),bcons,k1,k2,queryKeywords.get(queryId.get(j)),index,avglength,doclengths,docFreq,totaldocs);
			ArrayList<String> unigramlaplace_ranks = computeForUnigramLaplace(queryId.get(j),queryKeywords.get(queryId.get(j)),index,doclengths,vocabsize);
*/
			SkipGramMinSpanForDoc(queryId.get(j),totaldocs,c,f,queryKeywords.get(queryId.get(j)),docFreq,okapi_ranks,maxlength);
		}

		Iterator keys = doclengths.keySet().iterator();
		PrintWriter pw = new PrintWriter("doclengths");
		while(keys.hasNext())
		{
			int id = (int)keys.next();
			pw.println(docs.get(id)+" "+doclengths.get(id) +" ");
		}
		pw.close();
	}

	private static HashMap<Integer, Double> computeForOkapiModel(int qnum,
			HashMap<Integer, HashMap<Integer, Double>> okapi) throws IOException {
		HashMap<Integer,Double> okapiresult = new HashMap<Integer,Double>();
		Iterator terms = okapi.keySet().iterator();
		while(terms.hasNext())
		{
			int term = (int) terms.next();
			Iterator documents = okapi.get(term).keySet().iterator();
			while(documents.hasNext())
			{
				int doc = (int) documents.next();
				Double initval = okapiresult.containsKey(doc) ? okapiresult.get(doc) : 0;
				okapiresult.put(doc, initval+okapi.get(term).get(doc));
			}
		}
		return okapiresult;
	}
	
	private static ArrayList<String> computeForUnigramLaplace(int qnum,
			ArrayList<Integer> terms, HashMap<Integer, HashMap<Integer, Integer>> index,
			HashMap<Integer, Integer> doclengths, long vocabsize) throws IOException {

		HashMap<Integer,Double> unigramLaplace = new HashMap<Integer,Double>();

		for(int term : getDistinct(terms))
		{
			Iterator docnames= doclengths.keySet().iterator();
			while(docnames.hasNext())
			{
				int dname = (int) docnames.next();	
				double initval=unigramLaplace.containsKey(dname) ? unigramLaplace.get(dname) : 0;
				int tf = index.get(term).containsKey(dname) ? index.get(term).get(dname) : 0;
				unigramLaplace.put(dname,initval+computeForUnigramLaplcaeForDoc(doclengths.get(dname),tf,vocabsize));
			}
		}
		return rankDocuments(qnum,unigramLaplace,"UnigramLaplace.txt",1000);		
	}

	private static double computeForUnigramLaplcaeForDoc(int d,int tf,long v) {
		return Math.log((tf+1)/(float)(d+v));
	}

	private static ArrayList<String> computeForokapiBM25(int qnum,
			float bcons, float k1, float k2, ArrayList<Integer> terms,
			HashMap<Integer, HashMap<Integer, Integer>> index,
			double avglength, HashMap<Integer, Integer> doclengths,
			HashMap<Integer, Integer> docFreq, int totaldocs) throws IOException {
		HashMap<Integer, Double> okapiBM25 = new HashMap<Integer,Double>();
		for(int term : getDistinct(terms))
		{
			Iterator docnames= index.get(term).keySet().iterator();
			int qtf = getquerytermfreq(term,terms);
			//System.out.println("okapi " + term);
			long df = docFreq.get(term);
			while(docnames.hasNext())
			{
				int dname = (int) docnames.next();	
				double initval=okapiBM25.containsKey(dname) ? okapiBM25.get(dname) : 0;
				okapiBM25.put(dname,initval+computeForokapiBM25ForDoc(qtf,bcons,k1,k2,totaldocs,doclengths.get(dname)
						,df,avglength,dname,index.get(term).get(dname)));			
			}
		}
		return rankDocuments(qnum,okapiBM25,"okapiBM25.txt",1000);
	}

	private static double computeForokapiBM25ForDoc(int tfwq, float bcons,
			float k1, float k2, int totaldocs, int doclength, long dfw,
			double avglength, int dname, int tfw) {
		double idf = Math.log((totaldocs+0.5)/(dfw+0.5));
		double constant = ((1-bcons) + bcons*(doclength/avglength));
		return (idf * computeconstant(tfw,k1,constant) * computeconstant(tfwq,k2,1));
	}

	private static double computeconstant(int tfw, float k, double constant) {
		return ((tfw + k * tfw)/(tfw + k * constant));		
	}

	private static int getquerytermfreq(int term, ArrayList<Integer> terms) {
		int count =0;
		for(int i=0;i<terms.size();i++)
			if(terms.get(i).equals(term)) count++;
		return count;
	}

	private static ArrayList<String> computeFortfidfModel(Integer qnum,
			HashMap<Integer, HashMap<Integer, Double>> okapi, int totaldocs,
			HashMap<Integer, Integer> docFreq) throws IOException {
		HashMap<Integer,Double> tfidfresult = new HashMap<Integer,Double>();
		Iterator terms = okapi.keySet().iterator();
		while(terms.hasNext())
		{
			int term = (int) terms.next();
			Iterator documents = okapi.get(term).keySet().iterator();
			while(documents.hasNext())
			{
				int doc = (int) documents.next();
				Double initval = tfidfresult.containsKey(doc) ? tfidfresult.get(doc) : 0;
				tfidfresult.put(doc, initval+(okapi.get(term).get(doc) * Math.log(totaldocs/(float)docFreq.get(term))));
			}
		}
		return rankDocuments(qnum,tfidfresult,"tf-idf.txt",1000);
	}


	private static Double computeOkapiForDoc(HashMap<Integer,Integer> doclengths,double avgdoclength,int docno, float tf) {
		return (tf/(tf + 0.5 + (1.5 * (doclengths.get(docno)/avgdoclength))));
	}

	private static void computeOkapiValues(HashMap<Integer, HashMap<Integer,Double>> okapi,
			List<Integer> terms, HashMap<Integer, HashMap<Integer, Integer>> index, double avgdoclength, int qnum, HashMap<Integer, Integer> doclengths) throws IOException {
		for(int term : terms)
		{
			Iterator docnames= index.get(term).keySet().iterator();
			HashMap<Integer,Double> h = new HashMap<Integer,Double>();
			while(docnames.hasNext())
			{
				int dname = (int) docnames.next();	
				h.put(dname,computeOkapiForDoc(doclengths,avgdoclength,dname,index.get(term).get(dname)));
				//			System.out.println(t.get(term) + "occurs in "+docs.get(dname) +" "+index.get(term).get(dname)+" times.");
			}
			okapi.put(term, h);
		}
	}

	private static ArrayList<Integer> getDistinct(ArrayList<Integer> terms) {
		Set<Integer> s =new HashSet<Integer>(terms);
		return new ArrayList<Integer>(s);
	}

	private static ArrayList<String> rankDocuments(int qnum,HashMap<Integer,Double> result,String filename,int num) throws IOException {
		ArrayList<Double> rankedscores = new ArrayList<Double>(result.values());
		ArrayList<Integer> rankedDocs = new ArrayList<Integer>(result.keySet());
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(docroot+filename,true)));
		ArrayList<String> rankedDocuments = new ArrayList<String>(50);
		for(int i=1;i<=1000 && !rankedDocs.isEmpty();i++)
		{
			int position = max(rankedscores);
			if(rankedscores.get(position) == 0) break;
			if(rankedDocuments.size() != num ) {
				rankedDocuments.add(docs.get(rankedDocs.get(position)));
			}
			out.println(qnum + " Q0 " + docs.get(rankedDocs.get(position))+" "+ i + " " + rankedscores.get(position) + " Exp");
			rankedscores.remove(position);
			rankedDocs.remove(position);
		}
		out.close();
		return rankedDocuments;
	}

	private static int max(ArrayList<Double> scores) {
		double max = -Double.MAX_VALUE;
		int position=0;
		for(int i=0; i<scores.size(); i++)
		{
			if(scores.get(i)>=max) 
			{
				position =i;
				max=scores.get(i);
			}
		}
		return position;
	}

	private static int getMax(String term,HashMap<Integer,String> t) {
		Collection<Integer> vals = terms.values();
		ArrayList<Integer> a = new ArrayList<Integer>();
		a.addAll(vals);
		Collections.sort(a);
		t.put(a.get(a.size()-1), term);
		return a.get(a.size()-1);
	}

	private static String removeSpecialChars(String query) {
		query=query.substring(0,query.trim().length()-1);
		query=query.replaceAll(",", "")
				.replaceAll("'", " ")
				.replaceAll("\\(", "")
				.replaceAll("\\)", "")
				.replaceAll("\"", "")
				.replaceAll("\\.", "");
		return query;
	}

	private static void extractStopwords(HashMap<String,Integer> h) throws IOException
	{
		System.out.println("****Fetching stop words*****");
		BufferedReader b = new BufferedReader(new FileReader(docroot+"stoplist.txt"));
		String stopword="";
		while((stopword=b.readLine())!=null)
			h.put(stopword.toLowerCase().trim(), 0);
		b.close();
	}

	private static void loadFromFile(HashMap<String, Integer> terms,
			HashMap<Integer, String> docs,HashMap<Integer,String> t) throws Exception 
	{
		BufferedReader br = new BufferedReader(new FileReader(docroot+"Term-ID-Mappings"));
		String line = "";
		while((line=br.readLine())!=null)
		{
			terms.put(line.split(" ")[0], Integer.parseInt(line.split(" ")[1]));
			t.put(Integer.parseInt(line.split(" ")[1]),line.split(" ")[0]);
		}
		br.close();
		br = new BufferedReader(new FileReader(docroot+"Doc-ID-Mappings"));
		line = "";
		while((line=br.readLine())!=null)
			docs.put(Integer.parseInt(line.split(" ")[0]),line.split(" ")[1]);
		br.close();
	}

	private static long fetchDocFreqOfAllTerms(RandomAccessFile f, Catalog c, 
			HashMap<Integer, Integer> docFreq, HashMap<Integer, Integer> doclengths,HashMap<Integer, String> t) throws IOException {
		Iterator keys = c.getTermKeySet().iterator();
		int totallength=0;
		//PrintWriter pw = new PrintWriter("outputs");
		while(keys.hasNext())
		{
			int id = (int)keys.next();
			String terminfo = fetchForIDFromIndex(f,c.get(id));
			//System.out.println(id+" " +terminfo.substring(0, 10));
			//docFreq.put(id,countDocuments(line));
			Pattern matchdocs = Pattern.compile("(\\$([^\\$]+))");
			Matcher m = matchdocs.matcher(terminfo);
			int numOfDocs=0;
			int docID = -1;
			while(m.find())
			{
				numOfDocs++;
				String docinfo = m.group(2);
				docID = Integer.parseInt(docinfo.substring(0, docinfo.indexOf("*")));
				int count = 1;//countTF(docinfo);
				int initlength = doclengths.containsKey(docID) ? doclengths.get(docID) : 0;
				doclengths.put(docID, initlength+count);
				totallength++;
			}
			if(docID!=-1) docFreq.put(id, numOfDocs);
		}
		return totallength;
	}

	private static int countDocuments(String line) {
		int count=0;
		for(int i=0;i<line.length();i++)
			if(line.charAt(i)=='$') count++;
		return count;
	}

	private static String fetchForIDFromIndex(RandomAccessFile f, Tuple tuple) throws IOException {
		byte b[] = new byte[(int) tuple.getSize()];
		f.seek((int) tuple.getOffset());
		f.read(b);
		return new String(b);
	}

	private static HashMap<Integer, Integer> retrievePostings(RandomAccessFile f,Catalog c,int term, HashMap<Integer, Integer> docFreq) 
			throws IOException 
			{
		HashMap<Integer,Integer> termFreq = new HashMap<Integer,Integer>();
		if(!c.contains(term)) return termFreq;
		else {
			String terminfo = fetchForIDFromIndex(f,c.get(term));
			Pattern matchdocs = Pattern.compile("(\\$([^\\$]+))");
			Matcher m = matchdocs.matcher(terminfo);
			while(m.find())
			{
				String docinfo = m.group(2);
				int docID = Integer.parseInt(docinfo.substring(0, docinfo.indexOf("*")));
				int count = countTF(docinfo);
				termFreq.put(docID, count);
			}
		}
		return termFreq;
			}

	private static int countTF(String docinfo) {
		int count =0;
		for(int i=0;i<docinfo.length();i++)
			if(docinfo.charAt(i)=='*') count++;
		return count;
	}

	private static void print(HashMap<Integer, Integer> docFreq) {
		Iterator keys = docFreq.keySet().iterator();
		while(keys.hasNext())
		{
			int id = (int)keys.next();
			System.out.println(id+" "+docFreq.get(id) +" ");
		}
	}

	private static void SkipGramMinSpanForDoc(int qnum,int totaldocs,Catalog c,RandomAccessFile f,ArrayList<Integer> qterms, HashMap<Integer, Integer> docFreq, HashMap<Integer, Double> okapi_ranks, int maxlength) throws IOException
	{
		HashMap<Integer,Integer> doccount = new HashMap<Integer,Integer>();
		HashMap<Integer,ArrayList<Integer>> h = new HashMap<Integer,ArrayList<Integer>>();
		qterms = fetchTopQterms(qterms,docFreq,totaldocs);
		Iterator<Integer> trms = qterms.iterator();
		while(trms.hasNext()){
			int trm = (int)trms.next();
			System.out.println(trm+" "+t.get(trm));
			if(!c.contains(trm)) continue;
			else {
				String terminfo = fetchForIDFromIndex(f, c.get(trm));
				Pattern p = Pattern.compile("(\\$([^\\$]+))");
				Matcher m = p.matcher(terminfo);
				while(m.find())
				{
					String docinfo = m.group(2);
					int docID = Integer.parseInt(docinfo.substring(0, docinfo.indexOf("*")));
					int count = doccount.containsKey(docID) ? doccount.get(docID) : 0;
					doccount.put(docID,count+1);
				}
			}
		}
		int num=0;
		Iterator<Integer> itr = doccount.keySet().iterator();
		while(itr.hasNext())
		{
			int docid = itr.next();
			int count = doccount.get(docid);
			ArrayList<Integer> arr = h.containsKey(count) ? h.get(count) : new ArrayList<Integer>();
			arr.add(docid);
			h.put(count, arr);
		}

		int max = qterms.size();
		ArrayList<Integer> docids = new ArrayList<Integer>();
		while(max!=0)
		{
			if(h.containsKey(max))
			{
				docids.addAll(h.get(max));
				System.out.println("For "+max);
				//if(max!=1) print(h.get(max));
				num+=h.get(max).size();
			}
			System.out.println("Having "+max+ " "+docids.size());
			max--;
		}
		
		/*ArrayList<Integer> temp = new ArrayList<Integer>();
		if(docids.size()>1000)
		{
			for(int i=1;i<docids.size() && i<1000;i++)
				temp.add(docids.get(i));
			docids = temp;
		}*/
		System.out.println("Query id : "+qnum);
		HashMap<Integer,Double> scores = new HashMap<Integer,Double>();
		for(int i=0;i<docids.size();i++)
			scores.put(docids.get(i),okapi_ranks.get(docids.get(i))*findProximityWithinDoc(docids.get(i),qterms,c,f,maxlength));
		rankDocuments(qnum,scores,"proximitySearch.txt",50);
	}

	private static ArrayList<Integer> fetchTopQterms(ArrayList<Integer> qterms,
			HashMap<Integer, Integer> docFreq,int totaldocs) {
		HashMap<Integer,Double> result = new HashMap<Integer,Double>();
		for(int i=0;i<qterms.size();i++)
			result.put(qterms.get(i), Math.log(totaldocs/docFreq.get(qterms.get(i))));
		ArrayList<Double> rankedscores = new ArrayList<Double>(result.values());
		ArrayList<Integer> rankedDocs = new ArrayList<Integer>(result.keySet());
		ArrayList<Integer> rankedDocuments = new ArrayList<Integer>(qterms.size());
		int num = qterms.size() > 5 ? 5 :  qterms.size();
		while(!rankedDocs.isEmpty() && rankedDocuments.size()<num)
		{
			int position = max(rankedscores);
			if(rankedscores.get(position) == 0) break;
			rankedDocuments.add(rankedDocs.get(position));
			rankedscores.remove(position);
			rankedDocs.remove(position);
		}
		//System.out.println(rankedDocuments.get(0)+" "+rankedDocuments.get(1)+" "+rankedDocuments.get(2)+" ");
		return rankedDocuments;
	}

	private static void print(ArrayList<Integer> arrayList) {
		int num=20;
		for(int i=0;i<arrayList.size();i++)
			{
			System.out.print(docs.get(arrayList.get(i)) + " ");
			if(num==0) {
				num=20;
				System.out.println();
			}
			else num--;
			}
	}

	private static double findProximityWithinDoc(Integer id,
			ArrayList<Integer> qterms, Catalog c, RandomAccessFile f, int maxlength) throws IOException {
		ArrayList<ArrayList<Integer>> pointers = new ArrayList<ArrayList<Integer>>();
		ArrayList<Integer> queryterms = new ArrayList<Integer>();
		for(int i=0;i<qterms.size();i++)
		{
			//System.out.println(qterms.get(i));
			//if(id==29561) System.out.println("Following positions for "+qterms.get(i));
			String terminfo = fetchForIDFromIndex(f,c.get(qterms.get(i)));
			Pattern p = Pattern.compile("(\\$"+id+"(\\*[^\\$]+))");
			Matcher m = p.matcher(terminfo);
			ArrayList<Integer> positions = new ArrayList<Integer>();
			while(m.find())
			{
				String[] posinfo = m.group(2).split("\\*");
				for(int j=0;j<posinfo.length;j++)
					{
					if(!posinfo[j].equals("")) positions.add(Integer.parseInt(posinfo[j]));
					if(id==29561) System.out.print(posinfo[j]+ " ");
					}
				break;
			}
			if(positions.size()!=0) {
				queryterms.add(qterms.get(i));
				pointers.add(positions);
				//System.out.println("Query : "+qterms.get(i)+" for doc "+ docs.get(id)+" "+id);
			}
		}
		Integer min = maxlength+1;
		while(pointers.size()>0){
			
			ArrayList<Integer> firstColumn = getFirstRow(pointers);
			min = findMinSpan(new ArrayList<Integer>(firstColumn),min);
			int minval=maxlength+1;
			int position =-1;
			for(int i=0;i<firstColumn.size();i++)
			{
				if(firstColumn.get(i)<minval) minval = firstColumn.get(i);
				position=i;
			}
			pointers.get(position).remove(0);
			if(pointers.get(position).size()==0) break;
		}
		//System.out.println("Min == "+min);
		return Math.log(queryterms.size()+0.01)*(1/(double)min);
	}

	private static int findMinSpan(ArrayList<Integer> firstColumn,int min) {
		
		Collections.sort(firstColumn);
		//System.out.println("Positions "+firstColumn.get(firstColumn.size()-1)+" "+firstColumn.get(0));
		int span = firstColumn.get(firstColumn.size()-1)-firstColumn.get(0);
		if(span==0) return min;
		if(span<min) {
			//System.out.println("span : "+span);
			return span;
		}
		else return min;
	}

	private static ArrayList<Integer> getFirstRow(ArrayList<ArrayList<Integer>> pointers) {
		ArrayList<Integer> arr = new ArrayList<Integer>();
		for(int i=0;i<pointers.size();i++)
			{
			arr.add(pointers.get(i).get(0));
			//System.out.println(pointers.get(i).get(0));
			}
		return arr;
	}


}
