# Agenda

- 个性化搜索

- 工具

- 说说发布

---

# 简单网Solr个性化搜索场景

- service.solr.KeyWordsSearchService 使用edismax语法查询，目前主要是带关键词搜索的场景在用，url /solr/select

- service.solr.ClientSearchService 使用customsearch handler查询，url /solr/customselect

---

# edismax个性化搜索

- 用于关键词搜索场景，增加调试参数debugQuery=on
- 
  <http://123.150.204.75:8002/?term_id=74&start=0&rows=120&sortby=region_ctr_0111_4&xks=12&debugQuery=true&edismax=true&qf=item_title%5E1.4%20shop_name%5E0.5&boost=&bfs=mul(max(__SORT__,0.0001),max(0.1,__SIMI__))>

	!python
	import urlparse
	s = "rows=120&wt=json&fl=%2A%2Cscore&start=0&version=2&fq=item_id%3A%5B0+TO+%2A%5D&fq=term_id%3A74&q=%2A%3A%2A&debugQuery=true&defType=edismax&qf=item_title%5E1.4+shop_name%5E0.5&bf=mul%28max%28region_ctr_0111_4%2C0.0001%29%2Cmax%280.1%2Ctagdist%28guang_tag_match%2C1%3A0.5%2C3%3A0.5%2C%29%29%29"
	urlparse.parse_qsl(s)
	
	[('rows', '120'),
     ('wt', 'json'),
     ('fl', '*,score'),
     ('start', '0'),
     ('version', '2'),
     ('fq', 'item_id:[0 TO *]'),
     ('fq', 'term_id:74'),
     ('q', '*:*'),
     ('debugQuery', 'true'),
     ('defType', 'edismax'),
     ('qf', 'item_title^1.4 shop_name^0.5'),
     ('bf',
      'mul(max(region_ctr_0111_4,0.0001),max(0.1,tagdist(guang_tag_match,1:0.5,3:0.5,)))')]

---

# edismax参数替换

- 替换参数里的\_\_SIMI\_\_, \_\_SORT\_\_, \_\_TAG\_\_
- 

    !java
    r.replace("__SIMI__", "tagdist(guang_tag_match,__TAG__)")
     .replace("__SORT__", sortField)
     .replace("__TAG__", tagMatch) - 
    
- 其中tagdist是custom function，后面介绍，guang_tag_match是第一个参数，表示字段名称， tagMatch是用户的tag值，如1:0.5,3:0.5,    
- +MatchAllDocsQuery(*:*) FunctionQuery(product(max(float(region_ctr_0111_4),const(1.0E-4)),max(const(0.1),tagdist(guang_tag_match,1:0.5|3:0.5))))

- 详细的解释
- 

    !python
    1.4496841 = (MATCH) sum of:
      0.70710677 = (MATCH) MatchAllDocsQuery, product of:
        0.70710677 = queryNorm
      0.7425773 = (MATCH) FunctionQuery(product(max(float(region_ctr_0111_4),const(1.0E-4)),max(const(0.1),tagdist(guang_tag_match,1:0.5|3:0.5)))), product of:
        1.0501629 = product(max(float(region_ctr_0111_4)=1.6158,const(1.0E-4)),max(const(0.1),tagsimi(1:0.5|3:0.5,3:0.7|6:0.3)=0.6499337))
        1.0 = boost
        0.70710677 = queryNorm
---

# Solr edismax Query

- ExtendedDisMax Query Parser
	<http://wiki.apache.org/solr/ExtendedDisMax>
- q fq
- defType=edismax
- fl=*,score
- qf=item_title^1.4 shop_name^0.5
- bf=func()

  等价于 bq=\_val\_:func()…，可以有多个bf参数，累加

- boost=func()

  bf累加后的值再乘以一个boost
  
---

# Solr edismax Query Functions

- ord/sum/mul/sub/div/mod/pow/log/abs/min/max/sqrt
- Math.* 调用java方法
- docfreq/termfreq/idf/tf/norm
- if/not/and/or/exists

---

# 自定义函数，以tagdist为例

- 实现 TagDistanceSourceParser <http://review.jcndev.com/#patch,sidebyside,20800,1,solr_customsearch/src/main/java/org/apache/solr/search/TagDistanceSourceParser.java>
- 实现 TagDistanceFunction <http://review.jcndev.com/#patch,sidebyside,20800,1,solr_customsearch/src/main/java/org/apache/solr/search/TagDistanceFunction.java		>
- 修改 solrconfig.xml (modules里的solrconfig.xslt)

     <valueSourceParser name="tagdist" class="org.apache.solr.search.TagDistanceSourceParser" /\>

- 打包部署 solr-customsearch

---

# TagDistanceSourceParser

e.g. tagdist(guang_tag_match,1:0.5,3:0.5,)

    !java
    public class TagDistanceSourceParser extends ValueSourceParser {
    @Override
    public ValueSource parse(FunctionQParser fp) throws ParseException {
        String tagsFieldName = fp.parseId();
        List<String> tagsList = new ArrayList<String>();
        for (String tag = fp.parseArg(); tag != null; tag = fp.parseArg()) {
            tagsList.add(tag);
        }

        return new TagDistanceFunction(tagsFieldName, tagsList);
    }

---

# TagDistanceFunction

    !java
    public DocValues getValues(Map context, IndexReader reader) throws IOException {
    final IndexReader rd = reader;

    return new DocValues() {
        @Override
        public float floatVal(int doc) {
            double score = calcDistance(rd, doc);
            return (float)score;
        }

        @Override
        public double doubleVal(int doc) {
            return (double)floatVal(doc);
        }

        @Override
        public String toString(int doc) {
            String docTagstr;
            try {
                Map<Integer, Double> docTags = getDocTagsFromDocId(rd, doc);
                docTagstr = tagsToString(docTags);
            } catch (IOException e) {
                docTagstr = "except";
            }
            return "tagsimi(" + tagsToString(userTags) + "," + docTagstr + ")=" + floatVal(doc);
        }
    };

---

# 个性化搜索 customsearch

- 实现CustomSearchHandler

- 实现CustomSearchQueryComponent

- 修改solrconfig.xslt <http://review.jcndev.com/#patch,sidebyside,21672,1,modules/solr-customsearch/control/solrconfig.xslt>

- 配置：
  * 前600个商品使用customsearch
  * 预取6000条
  * custom search cache，4096条
    
---

# CustomSearchHandler

    !java
    public class CustomSearchHandler extends SearchHandler implements SolrCoreAware {
    @Override
    protected List<String> getDefaultComponents()
    {
        ArrayList<String> names = new ArrayList<String>(6);
        names.add( CustomSearchQueryComponent.COMPONENT_NAME ); // custom querycompent
        names.add( FacetComponent.COMPONENT_NAME );
        names.add( MoreLikeThisComponent.COMPONENT_NAME );
        names.add( HighlightComponent.COMPONENT_NAME );
        names.add( StatsComponent.COMPONENT_NAME );
        names.add( DebugComponent.COMPONENT_NAME );
        return names;
    }

---    

# CustomSearchQueryComponent

    !java
    @Override
    public void init( NamedList args ) {
        pool = DbPoolConnection.getInstance().getConnectionPool();
        CustomMBeanManager.registerMBean();
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
        double start = System.nanoTime();
        boolean isError = false;

        CustomSearchProcessor customSearchProcessor = new CustomSearchProcessor(rb);
        if (customSearchProcessor.process(this, pool)) {
            doPrefetch(rb);
        } else {
            isError = true;
            super.process(rb);
        }

        double spent = (System.nanoTime() - start) / 1000000.0;
        stats.addTotal(isError, spent);
        if (spent > SLOW_QUERY_IN_MS) {
            stats.addSlow(isError, spent);
        }
    }

    @Override
    public NamedList getStatistics() {
        return stats.getStats();
    }

    public void proxyProcess(ResponseBuilder rb) throws IOException {
        super.prepare(rb);
        super.process(rb);
    }
    
---
# CustomSearchProcessor

    !java
    public boolean process(CustomSearchQueryComponent queryComponent, PooledDataSource pool) {
        try {
            profiler.start("iscustomsearch");
            isUseCustomCache = config.getBool(Constant.IS_CUSTOM_CACHE_CONFIG, true);
            if (isUseCustomSearch()) {
                profiler.start("getuser");
                Map<Integer, Double> userTags = CustomUser.parseStrTagProbs(strTagProbs);
                user = CustomUser.createCustomUser(userId, yyid,
                        isUseCustomCache ? searcher : null, userTags, pool, customSearchInfo);
                if (user != null && user.isLoadSuccess()) {
                    processCustomSearch(queryComponent);
                    return true;
                } else if (isDebug) {
                    rb.rsp.add("DebugInfo", "User not loaded");
                }
            } else if (isDebug) {
                rb.rsp.add("DebugInfo", "Not use custom search");
            }
        } catch (Exception e) {
            log.error("Custom search failed, {}", StackTraceUtil.getStackTrace2(e));
            if (isDebug) {
                rb.rsp.add("DebugInfo", StackTraceUtil.getStackTrace2(e));
            }
        }
        profiler.stop();
        return false;
    }

---
# CustomSearchProcessor

    !java
    // fetch custom field to score
        SolrQueryRequest solrReq = buildCustomQueryRequest();
        try {
            CachedCustomSearch results;
            if (isUseCustomCache) {
                // try fetch from cache
                results = (CachedCustomSearch)searcher.cacheLookup(Constant.CUSTOM_CACHE_NAME, solrReq.getParamString());
                customSearchInfo.setResultCacheHit(results != null);
                if (isDebug) {
                    customSearchInfo.setCustomQuery(solrReq.getParamString());
                }
                if (results == null) {
                    profiler.start("newsearch");
                    results = internalSearchMoreResult(solrReq, sortFields, queryComponent);
                    searcher.cacheInsert(Constant.CUSTOM_CACHE_NAME, solrReq.getParamString(), results);
                    if (isDebug) {
                        log.debug("Custom Search Result {}", results);
                    }
                } else {
                    log.debug("Custom search cache hit {}", solrReq.getParamString());
                }
            } else {
                results = internalSearchMoreResult(solrReq, sortFields, queryComponent);
            }

            profiler.start("sort");
            // sort by custom sort handler
            DocSlice docSlice = sortAndSublistResult(results, sortOrder);
            checkDuplicateSlices(docSlice);

            profiler.start("response");
            // cut by rowCount
            DocListAndSet out = new DocListAndSet();
            out.docList = docSlice;
            // set results
            rb.setResults(out);

            SolrQueryResponse rsp = rb.rsp;
            rsp.add("response", rb.getResults().docList);
            rsp.getToLog().add("hits", rb.getResults().docList.matches());
            rsp.getToLog().add("isrchit", customSearchInfo.isResultCacheHit());
            rsp.getToLog().add("isuchit", customSearchInfo.isUserCacheHit());
            rsp.getToLog().add("custparams", solrReq.getParamString());
            if (isDebug) {
                rsp.add("DebugInfo", customSearchInfo.toString());
            }
            profiler.stop();
            if (profiler.elapsedTime() > MAX_SPENT_IN_NS_LOGGING) {
                log.info("Profiler info: {}", profiler.toString());
            } else {
                log.debug("Profiler info: {}", profiler.toString());
            }
        } finally {
            solrReq.close();
        }
---
# 问题

- 目前edismax性能太慢
  - 预先计算选款师相似度，保存到索引
- customselect里，二次搜索排序算法无法使用solr的标注edismax语法，需要手写
- 目前个性化参数都是solr的函数传入的，未来个性化参数可能非常复杂（目前lda已经30个topic了），需要开发loaduserdata函数在solr里读取redis，返回个性化数据
- 排序时取得score值，然后限制bf范围

---
# 工具

- taobao-crawler/tools/solr_test.py 

- taobao-crawler/neoguang/solrweb.py

- sdl-guang-admin
-

    !python
    python solrweb.py 0.0.0.0:8002 --stderr --color --verbose debug --dbhost 192.168.32.10

---
# 腾讯说说登录，保存cookie

- taobao-crawler/qzone/qq_login_proxy.py

- 可选方案，在服务器模拟javascript登录流程。缺点，如果qq修改javascript需要跟着改登录代码

- 目前的原理，修改hosts（114.112.164.92 test.qq.com），冒充test.qq.com。页面代理qq登录流程，用户登录成功后保存用户cookie到数据库。优点，代理模式，腾讯修改javascript基本不需要跟着修改代码。

- sdl-guang-script1
-

    !bash
    /usr/bin/python /usr/local/bin/qq_login_proxy.py 8025 --verbose debug --use_logfile --dbhost 192.168.33.161 --db guangbi --dbuser guangbi --dbpasswd guangbi --webdebug --qqhost test.qq.com --daemon

---
# qq_login_proxy.py

    !python
    class qqlogin:
        QQ_REFER = "http://www.qq.com/"
        headers = {'User-Agent' : DEFAULT_UA, 'Referer' : QQ_REFER}

        def GET(self):
            self.QQLOGIN_URL = "http://ui.ptlogin2.qq.com/cgi-bin/login?hide_title_bar=0&low_login=0&qlogin_auto_login=1&no_verifyimg=1&link_target=blank&appid=636014201&target=self&s_url=http%3A//" + FLAGS.qqhost + ":" + str(FLAGS.qqport) + "/qq2012/loginSuccess.htm"

            args = web.input(id='', passwd='langtaojin')
            opener = get_cookie_opener(is_accept_ending=False)
            req = urllib2.Request(self.QQLOGIN_URL, headers=self.headers)
            u = opener.open(req)
            result = u.read()
            pos = result.find('</html>')
            result = result[:pos] + '<script>document.getElementById("u").value="' + args['id'].encode('utf-8') +             '";document.getElementById("p").value="' + args['passwd'].encode('utf-8') + '";</script></html>'
            web.header("Content-Type", "text/html; charset=utf-8")
            return result

---
# qq_login_proxy.py

	!python
	class qqloginsuccess:
        def GET(self):
            #args = web.input()
            cookies = web.cookies()
            web.header("Content-Type", "text/html; charset=utf-8")
            cookie_content = generate_cookiefile(cookies)

            qqid = cookies['ptui_loginuin']
            db = web.database(dbn='mysql', db='guangbi', user='guangbi', pw='guangbi', port=FLAGS.dbport, host=FLAGS.dbhost)
            logger.info("updating database")
            db.update('wb_qq_account', where='qqid=%s' % qqid, last_login=datetime.datetime.now(), cookies=cookie_content)
            logger.info("updated database")

            open(FLAGS.dumpcookiepath, 'w').write(cookie_content)
            # remove any cookies
            web.setcookie('pgv_pvid', '', -1)
            web.setcookie('o_cookie', '', -1)
            return generate_cookiefile(cookies).replace("\n", "<br>")

---
# 腾讯说说自动发布

- taobao-crawler/qzone/post.py

- sdl-guang-admin，需要访问本地文件
-

    !python
    /usr/bin/python /usr/local/bin/post.py --db guangbi --dbuser guangbi --dbpasswd guangbi --fromdb --dbhost 192.168.33.161 --nouse_paperboy --pbcate shuoshuo --pbverbose warning --loop --use_logfile --commitfail --verbose info --timer --statshost 192.168.32.157 --stderr

---
# 腾讯说说自动发布

    !python
    def post_shuoshuo(cookiefile, photofile, content, sid=0, schedule_ts=0, post_id=0):
        qqid = get_cookie_value(cookiefile, "ptui_loginuin")
        photo_json = upload_photo2(cookiefile, photofile, qqid, sid)

        if photo_json.has_key('error'):
            logger.error("Post failed qq %s, xks %s, %s, %s, %s --> %s reason %s", qqid, sid, cookiefile, photofile, content, photo_json['error'], photo_json['msg'].encode('utf8'))
            log_post_error(post_id, "%s : %s" % (photo_json['error'], photo_json['msg'].encode('utf8')))
            return False
        albumid = photo_json['albumid']
        photoid = photo_json['lloc']
        width = photo_json['width']
        height = photo_json['height']
        special_url = photo_json['micro']['url']
        #TODO: save into wb_photojson if failed

        post_result = post_content(cookiefile, qqid, content, albumid, photoid, width, height, special_url, schedule_ts)
        #import pdb; pdb.set_trace()
        if (not schedule_ts and post_result.find("content_box") > 0):
            result = True
        elif schedule_ts:
            content_json = extract_json_from_html(post_result, 'frameElement.callback')
            if content_json['code'] < 0:
                logger.warn("Post failed qq %s, xks %s, %s, %s, %s, %s, %s, %s", qqid, sid, cookiefile, photofile, content, content_json['message'].encode('utf8'), content_json['code'], content_json['subcode'])
                log_post_error(post_id, "%s,%s : %s" % (content_json['code'], content_json['subcode'], content_json['message'].encode('utf8')))
                if content_json['code'] == -3000:
                    log_paperboy('Need login(content) xks %s' % sid)
                delete_photo(cookiefile, qqid, photo_json, sid)
            elif content_json.has_key('richinfo'):
                result = True
        else:
            logger.warn("Post failed qq %s, xks %s, %s, %s, %s", qqid, sid, cookiefile, photofile, content)
            delete_photo(cookiefile, qqid, photo_json, sid)
            log_post_error(post_id, "-2 : %s" % post_result)
        if (not schedule_ts and post_result.find("content_box") > 0) or (schedule_ts and post_result.find("richinfo") > 0):
            result = True

--- 
# 主要的注意的点和坑

- upload_photo2 不同的qq用户对应不同的host，如果向其他host开头的url发可能会一直失败
- 上传照片和说说是不同的host，可能上传照片成功，发说说失败，这时候要删除照片，否则相册会溢出
- 要注意相册的空间占用，目前靠人工

---
# 目前主要问题

- 有时候网络故障，会卡死在发送某一条说说上
  - 待解决：采用进程fork，增加超时，如果超时则kill掉
- 有时候网络不稳定，明明发送成功了却报网络故障，这时候客户端以为发送没成功，会再发一遍，导致重复
  - 待解决：发现问题的时候需要再去爬一下说说定时列表页面，如果发现已经发送成功则不再发送
- 登录用户有时候错乱
  - 已解决：先点一下用户名，再登录不会有问题
