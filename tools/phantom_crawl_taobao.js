
var fs = require('fs'),
    webpage = require('webpage'),
    system = require('system');
    
var default_ua = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)";

var url_filename = system.args[1];

String.prototype.trim=function(){return this.replace(/^\s\s*/, '').replace(/\s\s*$/, '');};
String.prototype.ltrim=function(){return this.replace(/^\s+/,'');};
String.prototype.rtrim=function(){return this.replace(/\s+$/,'');};

var extract_domain = function(url) {
    return url.replace('http://','').replace('https://','').split(/[/?#]/)[0];
}

var crawl_url = function(lines, pos) {
    console.log(">>> " + lines[pos]);
    var page = webpage.create();
    page.settings.userAgent = default_ua;

    page.onNavigationRequested = function(url, type, willNavigate, main) {
        //console.log(".. " + url);
        if ((url.indexOf('http://s.click.taobao.com')!=0) &&
            (url.indexOf('http://store.taobao.com')!=0) &&
            (url.indexOf('http://www.taobao.com')!=0)) {
                console.log(extract_domain(url));
                //phantom.exit();
                page.close();
                crawl_url(lines, pos+1);
            }
    };

    page.open(lines[pos], function(status) {});
}

//crawl_url(system.args[1]);
try {
    var content = fs.read(url_filename);
    var lines = content.split('\n');
    var pos = 0;
    crawl_url(lines, pos)
} catch (e) {
    //console.log(e);
}


