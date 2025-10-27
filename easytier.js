(function(){
'use strict';

// ===================== 配置注入与常量 =====================
var injectedServer  = (typeof window !== 'undefined' && ((window.FIXED_SERVER && typeof window.FIXED_SERVER === 'object' && window.FIXED_SERVER) || (window.FIXED_SERVER && typeof window.FIXED_SERVER === 'object' && window.FIXED_SERVER))) || null;
var injectedNetwork = (typeof window !== 'undefined' && (typeof window.FIXED_NETWORK === 'string' && window.FIXED_NETWORK || typeof window.FIXED_NETWORK === 'string' && window.FIXED_NETWORK)) || null;

var ICE = (function(){
  var base = [
    {urls:'stun:stun.l.google.com:19302'},
    {urls:'stun:stun1.l.google.com:19302'},
    {urls:'stun:global.stun.twilio.com:3478'}
  ];
  var override = (typeof window !== 'undefined') && ((window.ICE_OVERRIDE && Array.isArray(window.ICE_OVERRIDE) && window.ICE_OVERRIDE) || (window.ICE_OVERRIDE && Array.isArray(window.ICE_OVERRIDE) && window.ICE_OVERRIDE));
  return override || base;
})();

// 功能开始：直传常量（覆盖）
var CHUNK = 512 * 1024;              // 每片 512KB（提升首片速度）
var PREVIEW_PCT = 1;                 // 预览阈值 = 1%
var PREVIEW_MIN = 512 * 1024;        // 兼容常量（逻辑按百分比）
var PREVIEW_MAX = 2 * 1024 * 1024;   // 兼容常量（逻辑按百分比）
var HIGH_WATER  = 1.5 * 1024 * 1024; // 发送高水位 1.5MB
var LOW_WATER   = 0.6 * 1024 * 1024; // 发送低水位 0.6MB
// 功能结束：直传常量（覆盖）

var PART_FLUSH  = 4 * 1024 * 1024;        // 分片进度落盘间隔
var CACHE_LIMIT = 300 * 1024 * 1024; // 完整缓存上限
var PART_TTL_MS = 7 * 24 * 3600 * 1000;
var DIAL_TIMEOUT_MS = 7000;

// ===================== 小工具 =====================
function now(){ return new Date().toLocaleTimeString(); }
function shortId(id){ id=String(id||''); return id ? id.substr(0,10)+'...' : '-'; }
function human(n){
  n = Number(n||0);
  if(n < 1024) return n+' B';
  if(n < 1024*1024) return (n/1024).toFixed(1)+' KB';
  if(n < 1024*1024*1024) return (n/1024/1024).toFixed(1)+' MB';
  return (n/1024/1024/1024).toFixed(1)+' GB';
}
function ext(name){ var m=String(name||'').match(/\.([a-z0-9]+)$/i); return m? m[1].toLowerCase():''; }
function guessMimeByExt(name){
  var e = (ext(name)||'').toLowerCase();
  var map = {
    mp4:'video/mp4', m4v:'video/mp4', mov:'video/quicktime',
    webm:'video/webm', ogv:'video/ogg', mkv:'video/x-matroska', ts:'video/mp2t',
    mp3:'audio/mpeg', m4a:'audio/mp4', aac:'audio/aac', wav:'audio/wav',
    ogg:'audio/ogg', oga:'audio/ogg', opus:'audio/opus',
    flac:'audio/flac', amr:'audio/amr', wma:'audio/x-ms-wma'
  };
  return map[e] || '';
}
function normalizeMime(raw, name){
  var m = String(raw||'').trim().toLowerCase();
  if (!m || m==='application/octet-stream'){
    var g = guessMimeByExt(name);
    return g || m || 'application/octet-stream';
  }
  return m;
}
function isImg(mime,name){
  return (String(mime||'').indexOf('image/')===0) || ['jpg','jpeg','png','gif','webp','bmp','heic','heif','avif','svg'].indexOf(ext(name))!==-1;
}
function isVid(mime,name){
  return (String(mime||'').indexOf('video/')===0) || ['mp4','webm','mkv','mov','m4v','avi','ts','3gp','flv','wmv'].indexOf(ext(name))!==-1;
}
function isAudio(mime,name){
  return (String(mime||'').indexOf('audio/')===0) || ['mp3','wav','ogg','oga','m4a','aac','flac','opus','amr','wma'].indexOf(ext(name))!==-1;
}
function canPlayVideo(mime,name){
  try{
    var v=document.createElement('video');
    if(!v || !v.canPlayType) return false;
    var type = normalizeMime(mime, name);
    if(!type) return false;
    var res = v.canPlayType(type);
    return !!res && res !== 'no';
  }catch(e){ return false; }
}
function canPlayAudio(mime,name){
  try{
    var a=document.createElement('audio');
    if(!a || !a.canPlayType) return false;
    var type = normalizeMime(mime, name);
    if(!type) return false;
    var res = a.canPlayType(type);
    return !!res && res !== 'no';
  }catch(e){ return false; }
}
function genIp(id){
  var h=0; id=String(id||'');
  for(var i=0;i<id.length;i++){ h=((h*31) + id.charCodeAt(i))>>>0; }
  return '10.144.'+(((h)&0xff)+1)+'.'+(((h>>8)&0xff)+1);
}
function getPeerParam(){
  var s=window.location.search; if(!s||s.length<2) return '';
  var m=s.match(/[?&]peer=([^&]+)/); return m? decodeURIComponent(m[1]):'';
}
function sha256Hex(buf){
  return crypto.subtle.digest('SHA-256', buf).then(function(d){
    var b=new Uint8Array(d), s=''; for(var i=0;i<b.length;i++){ s+=('0'+b[i].toString(16)).slice(-2); }
    return s;
  });
}
function fileHashMeta(file){
  var headSize = Math.min(file.size||0, 256*1024);
  return new Promise(function(resolve){
    try{
      var r = new FileReader();
      r.onload = function(e){
        try{
          var head = new Uint8Array(e.target.result||new ArrayBuffer(0));
          var meta = new TextEncoder().encode([file.name||'', String(file.size||0), String(file.lastModified||0), ''].join('|'));
          var buf = new Uint8Array(meta.length + head.length);
          buf.set(meta,0); buf.set(head, meta.length);
          sha256Hex(buf).then(resolve).catch(function(){ resolve(''); });
        }catch(er){ resolve(''); }
      };
      r.onerror = function(){ resolve(''); };
      r.readAsArrayBuffer(file.slice(0, headSize));
    }catch(e){ resolve(''); }
  });
}
function formatTime(sec){
  sec = Math.max(0, Math.floor(sec||0));
  var h = Math.floor(sec/3600), m = Math.floor((sec%3600)/60), s = sec%60;
  var mm = (m<10?'0':'')+m, ss=(s<10?'0':'')+s;
  if (h>0) return (h<10?'0':'')+h+':'+mm+':'+ss;
  return mm+':'+ss;
}
function initialChar(name, mine){
  var n = String(name||'').trim();
  if (n) return n[0].toUpperCase();
  return mine ? '我' : '他';
}

// ===================== IndexedDB 缓存 =====================
var idb, idbReady=false;
(function openIDB(){
  try{
    var req = indexedDB.open('p2p-cache', 3);
    req.onupgradeneeded = function(e){
      var db=e.target.result;
      if(!db.objectStoreNames.contains('files')) db.createObjectStore('files',{keyPath:'hash'});
      if(!db.objectStoreNames.contains('parts')) db.createObjectStore('parts',{keyPath:'hash'});
    };
    req.onsuccess = function(e){ idb=e.target.result; idbReady=true; try{ idbCleanupStaleParts(); }catch(er){} };
    req.onerror = function(){ idbReady=false; };
  }catch(e){ idbReady=false; }
})();

function idbPutFull(hash, blob, meta){
  if(!idbReady || !hash) return;
  try{
    idbCleanupIfNeeded(meta && meta.size || 0);
    var tx=idb.transaction('files','readwrite');
    tx.objectStore('files').put({hash:hash, blob:blob, meta:meta, ts:Date.now()});
  }catch(e){}
}
function idbGetFull(hash, cb){
  if(!idbReady) return cb(null);
  try{
    var tx=idb.transaction('files','readonly');
    var rq=tx.objectStore('files').get(hash);
    rq.onsuccess=function(){ cb(rq.result||null); };
    rq.onerror=function(){ cb(null); };
  }catch(e){ cb(null); }
}
function idbPutPart(hash, meta){
  if(!idbReady || !hash) return;
  try{
    var tx=idb.transaction('parts','readwrite');
    tx.objectStore('parts').put({hash:hash, meta:meta, ts:Date.now()});
  }catch(e){}
}
function idbGetPart(hash, cb){
  if(!idbReady) return cb(null);
  try{
    var tx=idb.transaction('parts','readonly');
    var rq=tx.objectStore('parts').get(hash);
    rq.onsuccess=function(){ cb(rq.result||null); };
    rq.onerror=function(){ cb(null); };
  }catch(e){ cb(null); }
}
function idbDelPart(hash){
  if(!idbReady || !hash) return;
  try{
    var tx=idb.transaction('parts','readwrite');
    tx.objectStore('parts').delete(hash);
  }catch(e){}
}
function idbCleanupIfNeeded(addedSize){
  if(!idbReady) return;
  try{
    var total=0, items=[];
    var tx=idb.transaction('files','readonly');
    var st=tx.objectStore('files');
    var rq=st.openCursor();
    rq.onsuccess=function(e){
      var cur=e.target.result;
      if(cur){
        var v=cur.value||{};
        var sz=(v.meta&&v.meta.size)||0;
        total += sz;
        items.push({hash:v.hash, ts:v.ts||0, size:sz});
        cur.continue();
      }else{
        total += addedSize||0;
        if(total > CACHE_LIMIT){
          items.sort(function(a,b){ return (a.ts||0)-(b.ts||0); });
          var need=total-CACHE_LIMIT, freed=0, dels=[];
          for(var i=0;i<items.length && freed<need;i++){ freed+=items[i].size||0; dels.push(items[i].hash); }
          if(dels.length){
            var tx2=idb.transaction('files','readwrite'), s2=tx2.objectStore('files');
            dels.forEach(function(h){ try{s2.delete(h);}catch(e){} });
          }
        }
      }
    };
  }catch(e){}
}
function idbCleanupStaleParts(){
  if(!idbReady) return;
  try{
    var nowTs = Date.now(), dels=[];
    var tx=idb.transaction('parts','readonly');
    var st=tx.objectStore('parts');
    var rq=st.openCursor();
    rq.onsuccess=function(e){
      var cur=e.target.result;
      if(cur){
        var v=cur.value||{};
        var ts=v.ts||0, meta=v.meta||{};
        var stale = (nowTs - ts > PART_TTL_MS);
        var invalid = !meta || typeof meta.got!=='number' || typeof meta.size!=='number' || meta.got>=meta.size;
        if (stale || invalid){ dels.push(v.hash); }
        cur.continue();
      }else{
        if(dels.length){
          var tx2=idb.transaction('parts','readwrite'), s2=tx2.objectStore('parts');
          dels.forEach(function(h){ try{s2.delete(h);}catch(e){} });
        }
      }
    };
  }catch(e){}
}

// ===================== 视频缩略图 =====================
function extractVideoThumbnail(file){
  return new Promise(function(resolve){
    try{
      var video=document.createElement('video');
      video.preload='metadata'; video.muted=true; video.playsInline=true;
      var url=URL.createObjectURL(file);
      var cleaned=false; function clean(){ if(cleaned) return; cleaned=true; try{URL.revokeObjectURL(url);}catch(e){} }
      var timeout = setTimeout(function(){ clean(); resolve(null); }, 4000);
      video.addEventListener('loadedmetadata', function(){
        try{ video.currentTime = Math.min(1, (video.duration||1)*0.1); }catch(e){ clearTimeout(timeout); clean(); resolve(null); }
      }, {once:true});
      video.addEventListener('seeked', function(){
        try{
          clearTimeout(timeout);
          var w=video.videoWidth||320, h=video.videoHeight||180, r=w/h|| (16/9), W=320, H=Math.round(W/r);
          var c=document.createElement('canvas'); c.width=W; c.height=H;
          var g=c.getContext('2d'); g.drawImage(video,0,0,W,H);
          var poster=c.toDataURL('image/jpeg',0.7);
          clean(); resolve(poster);
        }catch(e){ clearTimeout(timeout); clean(); resolve(null); }
      }, {once:true});
      video.addEventListener('error', function(){ clearTimeout(timeout); clean(); resolve(null); }, {once:true});
      video.src=url;
    }catch(e){ resolve(null); }
  });
}

// ===================== 媒体中心（互斥播放 + 进度记忆） =====================
var MediaCenter = (function(){
  var list = new Set();
  var throttleTs = 0;

  function register(el, opts){
    if(!el) return;
    var item = { el: el, hash: (opts&&opts.hash)||'', name:(opts&&opts.name)||'', seek:null, timeEl:null, playBtn:null, noteEl:null, btnStyle:(opts&&opts.btnStyle)||'text' };
    if (opts && opts.seek) item.seek = opts.seek;
    if (opts && opts.timeEl) item.timeEl = opts.timeEl;
    if (opts && opts.playBtn) item.playBtn = opts.playBtn;
    if (opts && opts.noteEl) item.noteEl = opts.noteEl;
    list.add(item);

    function pauseOthers(){
      list.forEach(function(it){
        if (it.el !== el){
          try{ it.el.pause(); }catch(e){}
          if (it.playBtn){
            if (it.btnStyle === 'text') it.playBtn.textContent = '▶';
            else { it.playBtn.classList.remove('spin'); it.playBtn.classList.remove('playing'); }
          }
        }
      });
    }
    function updateTimeUI(){
      try{
        var dur = isFinite(el.duration) ? el.duration : 0;
        var cur = isFinite(el.currentTime) ? el.currentTime : 0;
        if (item.seek && dur>0){
          var v = Math.round(cur * 1000 / dur);
          if (!item.seek._dragging) item.seek.value = String(Math.max(0,Math.min(1000,v)));
        }
        if (item.timeEl){
          item.timeEl.textContent = formatTime(cur) + ' / ' + (dur>0?formatTime(dur):'--:--');
        }
      }catch(e){}
    }
    function saveProgressThrottled(){
      var n = Date.now();
      if (n - throttleTs < 400) return;
      throttleTs = n;
      try{
        if (item.hash){
          var key = 'media:'+item.hash;
          var payload = {t: el.currentTime||0, d: el.duration||0, n: item.name||''};
          localStorage.setItem(key, JSON.stringify(payload));
        }
      }catch(e){}
    }
    function restoreProgress(){
      try{
        if (!item.hash) return;
        var raw = localStorage.getItem('media:'+item.hash);
        if (!raw) return;
        var v = JSON.parse(raw||'{}');
        var t = Number(v && v.t || 0);
        if (isFinite(t) && t>0){
          var dur = (isFinite(el.duration)&&el.duration>0) ? el.duration : t;
          el.currentTime = Math.min(t, dur);
        }
      }catch(e){}
    }

    el.addEventListener('play', function(){
      pauseOthers();
      if (item.playBtn){
        if (item.btnStyle === 'text') item.playBtn.textContent='⏸';
        else { item.playBtn.classList.add('spin'); item.playBtn.classList.add('playing'); }
      }
    });
    el.addEventListener('pause', function(){
      if (item.playBtn){
        if (item.btnStyle === 'text') item.playBtn.textContent='▶';
        else { item.playBtn.classList.remove('spin'); item.playBtn.classList.remove('playing'); }
      }
    });
    el.addEventListener('timeupdate', function(){ updateTimeUI(); saveProgressThrottled(); });
    el.addEventListener('loadedmetadata', function(){ updateTimeUI(); restoreProgress(); });
    el.addEventListener('ended', function(){ updateTimeUI(); });

    if (item.seek){
      item.seek.addEventListener('input', function(){
        item.seek._dragging = true;
        try{
          var dur = isFinite(el.duration)?el.duration:0;
          var v = Number(item.seek.value||'0');
          if (dur>0){
            var t = Math.max(0, Math.min(dur, Math.round(dur * v / 1000)));
            el.currentTime = t;
          }
        }catch(e){}
      });
      item.seek.addEventListener('change', function(){ item.seek._dragging = false; });
    }
    if (item.playBtn){
      item.playBtn.addEventListener('click', function(){
        try{
          if (el.paused) el.play().catch(function(){});
          else el.pause();
        }catch(e){}
      });
    }
    return item;
  }

  function attachHash(el, hash, name){
    try{
      list.forEach(function(it){
        if (it.el === el){
          it.hash = hash||'';
          it.name = name||it.name||'';
          if (isFinite(el.duration) && el.duration>0){
            try{
              var raw = localStorage.getItem('media:'+it.hash);
              if (raw){
                var v = JSON.parse(raw||'{}');
                var t = Number(v && v.t || 0);
                if (isFinite(t) && t>0) el.currentTime = Math.min(t, el.duration);
              }
            }catch(e){}
          }
        }
      });
    }catch(e){}
  }

  return { register: register, attachHash: attachHash };
})();

// ===================== 应用主体 =====================
var app=(function(){
  var self={};

  self.server  = injectedServer || {host:'peerjs.92k.de', port:443, secure:true, path:'/'};
  self.network = injectedNetwork || 'public-network';
  self.iceServers = ICE;

  self.chunkSize  = CHUNK;
  self.previewPct = PREVIEW_PCT;
  self.previewMin = PREVIEW_MIN;
  self.previewMax = PREVIEW_MAX;
  self.highWater  = HIGH_WATER;
  self.lowWater   = LOW_WATER;

  self.peer=null; self.conns={}; self.isConnected=false; self.startAt=0;
  self.localId=''; self.virtualIp='';
  self.timers={up:null,ping:null};
  self.logBuf='> 初始化：准备连接'; self.logFullBuf=self.logBuf;
  self.fullSources={}; self.displayNames={}; self.activePeer='all';
  self.myName = (localStorage.getItem('nickname')||'').trim() || '';
  self.uiRoot = null;
  self._muted = false;
  self._sendTrackers = {}; // gid -> {ui,total,done,name,size}
  self._seenMsgIds = new Set(); // 群聊去重
  self._debug = true; // 详细日志开关（默认开启）
  self._dcMaxBytes = 256 * 1024; // DataChannel
  self._connecting = false;
  self._seedIds = new Set();
  self.__autostarted = false;

  // 功能开始：默认播放模式（严格分离）
  self.streamMode = false;
  try{ localStorage.setItem('streamMode','0'); }catch(e){}
  self._streams = {};          // sid -> { call, pid, kind, fid, localEl? }
  self._streamUiByFid = {};    // fid -> ui
  self.setStreamMode = function(flag){
    self.streamMode = !!flag;
    try{ localStorage.setItem('streamMode', self.streamMode ? '1' : '0'); }catch(e){}
    try{
      var chip = document.getElementById('streamChip');
      if (chip) chip.textContent = self.streamMode ? '在线播放' : '缓存观看';
    }catch(e){}
    if (!self.streamMode){
      try{
        Object.keys(self._streams).forEach(function(sid){
          var it = self._streams[sid];
          try{ it.call && it.call.close(); }catch(e){}
          delete self._streams[sid];
        });
      }catch(e){}
    }
    self.log('MODE_SWITCH: '+(self.streamMode?'在线播放':'缓存观看'));
  };
  // 功能结束：默认播放模式

  function isImportant(s){
    var t=String(s||'');
    return /已连接|断开|错误|拨号|入站|消息|文件|开始连接|连接超时|连接已关闭|发送|接收|通话|延迟|直播|ADOPT|DROP|BIN/i.test(t);
  }
  function log(s){ // 统一日志
    var line="["+now()+"] "+s;
    self.logFullBuf += "\n"+line;
    if (isImportant(s) || self._debug){
      self.logBuf += "\n"+line;
      var el=document.getElementById('log');
      if(el){ el.textContent=self.logBuf; el.scrollTop=el.scrollHeight; }
    }
    if (typeof window.updateEntryStatus === 'function'){
      var up='00:00:00';
      if(self.isConnected && self.startAt){
        var sec=Math.floor((Date.now()-self.startAt)/1000), h=Math.floor(sec/3600), m=Math.floor((sec%3600)/60), s2=sec%60;
        up=(h<10?'0':'')+h+':'+(m<10?'0':'')+m+':'+(s2<10?'0':'')+s2;
      }
      window.updateEntryStatus({
        connected:self.isConnected,
        online:Object.keys(self.conns).filter(function(k){return self.conns[k].open;}).length,
        localId:self.localId, virtualIp:self.virtualIp, uptime:up
      });
    }
  }
  self.log = log;

  self.copyLog=function(){
    try{
      var txt=self.logFullBuf||'';
      if(navigator.clipboard&&navigator.clipboard.writeText){
        navigator.clipboard.writeText(txt).then(function(){ alert('已复制全部日志'); });
      }else{
        var ta=document.createElement('textarea'); ta.value=txt;
        document.body.appendChild(ta); ta.select(); document.execCommand('copy');
        document.body.removeChild(ta); alert('已复制全部日志');
      }
    }catch(e){ alert('复制失败：'+e.message); }
  };
  self.clearLog=function(){
    self.logBuf=''; self.logFullBuf='';
    var el=document.getElementById('log'); if(el) el.textContent='';
  };
  function setStatus(txt){
    var st=document.getElementById('statusChip');
    if(st) st.textContent = self.isConnected ? '已连接' : (txt && txt.indexOf('在线')!==-1?'已连接': (txt?('状态：'+txt):'未连接'));
  }
  self.updateInfo=function(){
    var openCount=0; for(var k in self.conns){ if(self.conns[k].open) openCount++; }
    var lid=document.getElementById('localId'),
        vip=document.getElementById('virtualIp'),
        pc=document.getElementById('peerCount');
    if(lid) lid.textContent = self.localId ? shortId(self.localId) : '-';
    if(vip) vip.textContent = self.virtualIp || '-';
    if(pc)  pc.textContent  = String(openCount);
    var onlineChip=document.getElementById('onlineChip');
    if(onlineChip) onlineChip.textContent='在线 '+openCount;
    if(self._classic && typeof self._classic.updateStatus==='function') self._classic.updateStatus();
  };
  self.showShare=function(){
    var base=window.location.origin+window.location.pathname;
    var url = base + '?peer='+encodeURIComponent(self.localId);
    var input=document.getElementById('shareLink'),
        qr=document.getElementById('qr');
    if(input) input.value=url;
    if(qr){
      qr.innerHTML='';
      if (typeof QRCode !== 'undefined'){
        new QRCode(qr,{text:url,width:256,height:256,correctLevel:QRCode.CorrectLevel.M});
      }
    }
    var share=document.getElementById('share'); if(share) share.style.display='block';
  };
  self.copyLink=function(){
    var el=document.getElementById('shareLink'); if(!el) return;
    try{
      if(navigator.clipboard&&navigator.clipboard.writeText){
        navigator.clipboard.writeText(el.value).then(function(){ alert('已复制'); });
      } else { el.select(); document.execCommand('copy'); alert('已复制'); }
    }catch(e){ alert('复制失败：'+e.message); }
  };

  // ====== UI 适配器（声明） ======
  function pushChat(text,mine,convId,senderName){
    if(self._classic && typeof self._classic.appendChatWithConv==='function') self._classic.appendChatWithConv(text,mine,convId||self.activePeer||'all', senderName|| (mine?self.myName:(self.displayNames[convId]||'')));
  }
  function placeholder(name,size,mine,convId,senderName){
    if(self._classic && typeof self._classic.placeholderWithConv==='function') return self._classic.placeholderWithConv(name,size,mine,convId||self.activePeer||'all', senderName|| (mine?self.myName:(self.displayNames[convId]||'')));
    return null;
  }
  function showImg(ui,url){ if(self._classic && typeof self._classic.showImage==='function') self._classic.showImage(ui,url); }
  function showVid(ui,url,note,poster,name,hash){ if(self._classic && typeof self._classic.showVideo==='function') self._classic.showVideo(ui,url,note,poster,name,hash); }
  function showAud(ui,url,note,name,hash){ if(self._classic && typeof self._classic.showAudio==='function') self._classic.showAudio(ui,url,note,name,hash); }
  function fileLink(ui,url,name,size){ if(self._classic && typeof self._classic.showFileLink==='function') self._classic.showFileLink(ui,url,name,size); }
  function updProg(ui,txt){ if(self._classic && typeof self._classic.updateProgressText==='function') self._classic.updateProgressText(ui,txt); }
  function mkUrl(blob){ return (self._classic && typeof self._classic.mkUrl==='function') ? self._classic.mkUrl(blob) : URL.createObjectURL(blob); }

  // ====== 发送文本 ======
  self.sendMsg=function(){
    var val='';
    if (self._classic && typeof self._classic.getEditorText==='function') val=self._classic.getEditorText();
    val = (val||'').trim();
    if (!val){ self.log('T14 OUT_ERROR: empty message'); return; }

    var to = (self.activePeer==='all') ? 'all' : self.activePeer;
    var msg = { type:'chat', text: val, to: to, from: self.localId, mid: String(Date.now())+'_'+Math.random().toString(36).slice(2,8), relay: false };

    pushChat(val, true, to==='all'?'all':to, self.myName);
    if (self._classic && typeof self._classic.clearEditor==='function') self._classic.clearEditor();

    var targets=[];
    if (to==='all'){ for (var k in self.conns){ if(self.conns.hasOwnProperty(k) && self.conns[k].open) targets.push(k); } }
    else { if (self.conns[to] && self.conns[to].open) targets=[to]; }
    if (!targets.length){ self.log('T14 OUT_ERROR: no open peers to send'); return; }

    self.log('CHAT_SEND_BEGIN: to='+to+' targets='+targets.length);
    targets.forEach(function(pid){
      try{ self.conns[pid].conn.send(msg); }
      catch(e){ self.log('T14 OUT_ERROR: chat send '+(e.message||e)); }
    });
    self.log('T40 CHAT_SENT: '+ (val.length>30? (val.slice(0,30)+'…') : val) +' -> '+targets.length+(to==='all'?'(群)':'(单)'));
  };

  // ====== 连接开关 ======
  self.toggle=function(){
    if(self.isConnected){ self.disconnect(); return; }
    if (self._connecting){ self.log('正在连接中，忽略重复触发'); return; }
    var nameEl=document.getElementById('networkName');
    if(nameEl && nameEl.value.trim()) self.network=nameEl.value.trim();
    var nick = (localStorage.getItem('nickname')||'').trim();
    self.myName = nick || ('用户-'+Math.random().toString(36).slice(2,6));
    connect();
  };

  function connect(){
    if (self._connecting){ self.log('正在连接中（去抖）'); return; }
    self._connecting = true;
    setStatus('连接中…'); self.log('开始连接…');

    // ========== 纯前端“种子节点池”机制 ==========
    var SEED_POOL = [
      'p2p-chat-seed-alpha-' + self.network,
      'p2p-chat-seed-beta-'  + self.network,
      'p2p-chat-seed-gamma-' + self.network
    ];
    var candidates = SEED_POOL.slice().sort(function(){ return Math.random() - 0.5; });
    self.log('SEED_POOL: '+JSON.stringify(SEED_POOL));

    (function tryOccupy(i){
      if (i >= candidates.length) { startAsNormalPeer(); return; }
      var mySeedID = candidates[i];
      self.log('尝试占据种子节点：' + mySeedID);

      try{
        var stage = 'occupying';
        var seedPeer = new Peer(mySeedID, {
          host:self.server.host, port:self.server.port, secure:self.server.secure, path:self.server.path||'/',
          config:{iceServers:self.iceServers, iceCandidatePoolSize:8, bundlePolicy:'max-bundle'}
        });
        var opened = false;
        var timer = setTimeout(function(){
          if (!opened && stage==='occupying'){
            self.log('占据 '+mySeedID+' 超时，换下一个');
            try{ seedPeer.destroy(); }catch(e){}
            tryOccupy(i+1);
          }
        }, 4000);

        seedPeer.on('open', function(id){
          opened = true; stage='opened'; clearTimeout(timer);
          self.log('✅ 成为种子节点：' + id);
          self._seedIds.add(id);
          self.peer = seedPeer; self.isSeedNode = true;
          proceedWithPeer(self.peer, SEED_POOL);
        });

        seedPeer.on('error', function(err){
          clearTimeout(timer);
          if (stage !== 'occupying'){
            self.log('种子运行期错误（忽略）：' + (err && (err.message||err.type) || err));
            return;
          }
          self.log('种子节点错误（'+mySeedID+'）：' + (err && (err.message||err.type) || err));
          try{ seedPeer.destroy(); }catch(e){}
          tryOccupy(i+1);
        });
      }catch(e){
        self.log('种子节点创建失败（'+mySeedID+'）：' + e.message);
        tryOccupy(i+1);
      }
    })(0);

    function startAsNormalPeer(){
      try{
        var p=new Peer(null,{host:self.server.host,port:self.server.port,secure:self.server.secure,path:self.server.path||'/',config:{iceServers:self.iceServers}});
        self.peer=p; self.isSeedNode=false;
        proceedWithPeer(p, SEED_POOL);
      }catch(e){ self.log('初始化失败：'+e.message); setStatus('离线'); self._connecting=false; }
    }
  }

  function proceedWithPeer(p, seedPool){
    var opened=false;
    var t=setTimeout(function(){
      if(!opened){
        self.log('连接超时');
        try{ p.destroy(); }catch(e){}
        setStatus('离线');
        self._connecting=false;
      }
    }, 10000);

    function onOpened(id){
      if (opened) return;
      opened=true; clearTimeout(t);
      self._connecting=false;
      self.localId=id; self.virtualIp=genIp(id); self.isConnected=true; self.startAt=Date.now();
      setStatus('在线');
      self.updateInfo();
      self.showShare();
      self.log('已连接，ID='+id+(self.isSeedNode?' [种子节点]':''));

      // 桥接其它种子
      var others = (Array.isArray(seedPool)?seedPool:[]).filter(function(s){ return s && s!==id; });
      if (others.length){
        self.log('连接其它种子以桥接… '+JSON.stringify(others));
        others.forEach(function(seedID, idx){
          setTimeout(function(){ connectPeer(seedID); }, 400 + idx*250);
        });
      }

      // 3 秒保底再拨
      setTimeout(function(){
        try{
          var anySeedOpen = Object.keys(self.conns).some(function(k){
            return self.conns[k] && self.conns[k].open && others.indexOf(k)!==-1;
          });
          if (!anySeedOpen && others.length){
            self.log('未连上任何种子，重试拨号(3s)');
            others.slice(0,6).forEach(function(sid, i){
              setTimeout(function(){ connectPeer(sid); }, i*200);
            });
          }
        }catch(e){}
      }, 3000);

      // 10 秒保底再拨
      setTimeout(function(){
        try{
          var anySeedOpen2 = Object.keys(self.conns).some(function(k){
            return self.conns[k] && self.conns[k].open && others.indexOf(k)!==-1;
          });
          if (!anySeedOpen2 && others.length){
            self.log('未连上任何种子，重试拨号(10s)');
            others.slice(0,6).forEach(function(sid, i){
              setTimeout(function(){ connectPeer(sid); }, i*200);
            });
          }
        }catch(e){}
      }, 10000);

      var toDial=getPeerParam();
      if(toDial){ self.log('准备连接对端：'+toDial); setTimeout(function(){ connectPeer(toDial); },1500); }

      startTimers();
    }

    p.on('open', function(id){ onOpened(id || p.id || ''); });
    // 已打开兜底
    if (p && (p.open || (typeof p.id==='string' && p.id))) { onOpened(p.id); }

    p.on('connection', function(conn){ handleConn(conn,true); });
    p.on('error', function(err){
      self.log('连接错误：'+(err && (err.message||err.type)||err));
      if (err && /Lost connection to server/i.test(String(err.message||err.type||''))){
        try{ p.reconnect(); }catch(e){}
      }
    });
    p.on('disconnected', function(){ self.log('信令掉线，尝试重连'); try{ p.reconnect(); }catch(e){} });
    p.on('close', function(){ self.log('连接已关闭'); });

    // 接收“文件直播”或入口页视频通话
    p.on('call', function(call){
      var meta = call && call.metadata || {};
      if (meta && meta.kind === 'file-stream'){
        try{ call.answer(); }catch(e){ try{ call.close(); }catch(_e){} return; }
        var fid = meta.fid, sid = meta.sid, kind = meta.media || 'video';
        var ui = self._streamUiByFid[fid];
        if (!ui){
          var convId = (self.conns[call.peer] && self.conns[call.peer].recv && self.conns[call.peer].recv.convId) || 'all';
          ui = placeholder(meta.name||'媒体', 0, false, convId, self.displayNames[call.peer] || ('节点 '+shortId(call.peer)));
          self._streamUiByFid[fid] = ui;
        }

        try{
          if (!ui.mediaEl){
            ui.mediaWrap.classList.add('media');
            if (kind==='video'){
              var posterAttr = meta.poster ? ' poster="'+meta.poster+'"' : '';
              ui.mediaWrap.innerHTML =
                '<video controls playsinline'+posterAttr+' style="width:var(--thumb);border-radius:8px;background:#000"></video>'
                + '<div class="progress-line">直播中…</div>';
              ui.mediaEl = ui.mediaWrap.querySelector('video');
            }else{
              ui.mediaWrap.innerHTML =
                '<audio controls style="width:var(--thumb)"></audio>'
                + '<div class="progress-line">直播中…</div>';
              ui.mediaEl = ui.mediaWrap.querySelector('audio');
            }
            ui.progress = ui.mediaWrap.querySelector('.progress-line');
          }else{
            updProg(ui, '直播中…');
          }
        }catch(e){}

        call.on('stream', function(remote){
          try{
            ui.mediaEl.srcObject = remote;
            ui.mediaEl.play().catch(function(){});
            updProg(ui, '直播中…');
          }catch(e){}
        });
        call.on('close', function(){
          try{ updProg(ui, '直播结束'); }catch(e){}
          try{ delete self._streams[sid]; }catch(_){}
        });
        call.on('error', function(){
          try{ updProg(ui, '直播错误'); }catch(e){}
          try{ delete self._streams[sid]; }catch(_){}
        });

        self._streams[sid] = { call:call, pid:call.peer, fid:fid, kind:kind };
        return;
      }

      if (!window.__ENTRY_PAGE__){ try{ call.close(); }catch(e){} return; }
      navigator.mediaDevices.getUserMedia({video:true,audio:true}).then(function(stream){
        self._media = self._media || {};
        self._media.local = stream;
        var lv=document.getElementById('localVideo'); if(lv) lv.srcObject=stream;
        call.answer(stream);
        self._media.call = call;
        call.on('stream', function(remote){ var rv=document.getElementById('remoteVideo'); if(rv) rv.srcObject=remote; });
        call.on('close', function(){ self.toggleCall(true); });
        call.on('error', function(){ self.toggleCall(true); });
      }).catch(function(){ try{ call.close(); }catch(e){} });
    });
  }

  function connectPeer(pid){
    if(!self.peer || !pid || pid===self.localId) return;

    // 拨号并发与退避
    self._maxDial = self._maxDial || 4;
    self._dialQ = self._dialQ || [];
    self._dialing = self._dialing || new Set();
    self._dialActive = self._dialActive || 0;
    self._dialAttempts = self._dialAttempts || {};
    self._dialBackoff = self._dialBackoff || {};

    // 已连/正在拨/退避中直接跳过
    var st=self.conns[pid];
    if(st && st.open) return;
    if(self._dialing.has(pid)) return;
    var now=Date.now(), until=self._dialBackoff[pid]||0;
    if(now<until){ setTimeout(function(){ connectPeer(pid); }, until-now+5); return; }

    // 入队（去重）
    if(self._dialQ.indexOf(pid)===-1) self._dialQ.push(pid);

    pump();

    function pump(){
      if(self._dialActive >= self._maxDial) return;
      var next = self._dialQ.shift();
      if(!next) return;

      var st2=self.conns[next];
      if(st2 && st2.open){ setTimeout(pump,0); return; }
      if(self._dialing.has(next)){ setTimeout(pump,0); return; }

      self._dialing.add(next);
      self._dialActive++;
      self.log('拨号：'+next);

      var c;
      try{
        c = self.peer.connect(next,{reliable:true, serialization:'binary'});
        self.log('DIAL_OUT: pid='+next+' opts=binary');
      }catch(e){
        self.log('拨号失败：'+(e.message||e));
        finalize(next,false,e);
        return;
      }

      handleConn(c,false);

      var timer=setTimeout(function(){
        var st3=self.conns[next];
        if(!st3||!st3.open){
          try{ c && c.close(); }catch(_){}
          self.log('对端未响应：'+shortId(next));
          finalize(next,false,new Error('timeout'));
        }
      }, DIAL_TIMEOUT_MS);

      c.on('open', function(){ clearTimeout(timer); finalize(next,true); });
      c.on('error', function(err){ clearTimeout(timer); finalize(next,false,err); });
      c.on('close', function(){ clearTimeout(timer); finalize(next, !!(self.conns[next]&&self.conns[next].open)); });
    }

    function finalize(id, ok, err){
      if(self._dialing.has(id)) self._dialing.delete(id);
      if(self._dialActive>0) self._dialActive--;
      var a=self._dialAttempts[id]||0;
      if(ok){ self._dialAttempts[id]=0; self._dialBackoff[id]=0; }
      else{
        a=Math.min(a+1,6);
        self._dialAttempts[id]=a;
        var wait=Math.min(15000, Math.round(400*Math.pow(1.8,a)));
        self._dialBackoff[id]=Date.now()+wait;
      }
      setTimeout(function(){ if(self._dialQ.length) pump(); },0);
    }
  }

  // ====== 发送/直传所需底层方法 ======
  function getDC(c){
    try{
      return c && (c._dc || c.dc || c.dataChannel || c._dataChannel) || null;
    }catch(e){ return null; }
  }
  function getBuffered(c){
    try{
      var dc = getDC(c);
      if (dc && typeof dc.bufferedAmount === 'number') return dc.bufferedAmount;
      if(c && typeof c.bufferSize==='number') return c.bufferSize;
    }catch(e){}
    return 0;
  }
// 功能开始：flowSend流控发送
function flowSend(c,data,cb){
  var dc = getDC(c);
  var useThreshold = false;
  try{
    useThreshold = !!(dc && typeof dc.bufferedAmount === 'number' && 'bufferedAmountLowThreshold' in dc);
    if (useThreshold && !dc.__flowCfg){
      try{
        var low = Math.max(64*1024, (typeof self.lowWater==='number'? self.lowWater : 0)|0);
        dc.bufferedAmountLowThreshold = low;
        dc.__flowCfg = 1; // only once
      }catch(e){}
    }
  }catch(e){}

  // 发送一个“切片”并处理流控
  function sendOne(slice, done){
    function reallySend(){
      try{ c.send(slice); }catch(err){ return done(err); }
      return done(null);
    }
    if (useThreshold){
      if (dc.bufferedAmount > (typeof self.highWater==='number'? self.highWater : (1.5*1024*1024))){
        if (self && self._debug) self.log('FLOW_WAIT: bufferedAmount='+dc.bufferedAmount);
        var onLow = function(){
          if (dc.bufferedAmount <= dc.bufferedAmountLowThreshold){
            try{ dc.removeEventListener('bufferedamountlow', onLow); }catch(e){}
            reallySend();
          }
        };
        try{ dc.addEventListener('bufferedamountlow', onLow); }catch(e){}
      }else{
        reallySend();
      }
    }else{
      var loop=function(){
        if(getBuffered(c) > (typeof self.highWater==='number'? self.highWater : (1.5*1024*1024))){ setTimeout(loop,10); return; }
        reallySend();
      };
      loop();
    }
  }

  // 判断是否为二进制（需要切片）
  var isAB = (data && data instanceof ArrayBuffer);
  var isBlob = (typeof Blob!=='undefined') && (data instanceof Blob);
  var isTA = (data && data.buffer && typeof data.byteLength==='number' && Object.prototype.toString.call(data).indexOf('Array')!==-1);

  // 非二进制：按旧逻辑一次发
  if (!isAB && !isBlob && !isTA){
    return sendOne(data, cb);
  }

  // 二进制：按上限切片发送
  var totalBytes = 0;
  if (isBlob){ totalBytes = data.size||0; }
  else if (isAB){ totalBytes = data.byteLength||0; }
  else { totalBytes = data.byteLength||0; } // TypedArray

  function currentMax(){
    var dflt = 256*1024;
    try{
      if (self && typeof self._dcMaxBytes==='number' && self._dcMaxBytes>0) return self._dcMaxBytes|0;
    }catch(e){}
    return dflt;
  }
  function setMax(n, reason){
    try{
      var old = self._dcMaxBytes||0;
      self._dcMaxBytes = Math.max(8*1024, n|0);
      if (self && self._debug) self.log('DC_MAX_ADJUST: from='+(old||'unset')+' to='+self._dcMaxBytes+' by="'+reason+'"');
    }catch(e){}
  }

  var offset = 0;

  function next(){
    if (offset >= totalBytes) return cb(null);

    var max = currentMax();
    var end = Math.min(offset + max, totalBytes);

    // 取切片
    var slice;
    if (isBlob){
      slice = data.slice(offset, end);
    } else if (isAB){
      slice = data.slice(offset, end);
    } else {
      // TypedArray -> ArrayBuffer 切片
      try{
        var buf = data.buffer;
        var start = (data.byteOffset||0) + offset;
        var stop  = (data.byteOffset||0) + end;
        slice = buf.slice(start, stop);
      }catch(e){
        slice = (data.buffer && data.buffer.slice) ? data.buffer.slice(offset, end) : new Uint8Array(0).buffer;
      }
    }

    sendOne(slice, function(err){
      if (err){
        var msg = String(err && (err.message||err) || '');
        // 从错误信息解析上限（例如 "maximum of 262144 bytes"）
        var m = msg.match(/maximum of\s+(\d+)\s*bytes/i);
        if (m){
          var n = parseInt(m[1],10);
          if (isFinite(n) && n>0){
            setMax(n, msg);
            return next(); // 用新的上限重试当前 offset
          }
        }
        // 保底再降到 64KB 尝试一次
        if (!self._dcMaxBytes || self._dcMaxBytes > 64*1024){
          setMax(64*1024, 'fallback');
          return next();
        }
        return cb(err);
      }
      offset = end;
      next();
    });
  }

  next();
}
// 功能结束：flowSend流控发送
  // ====== 发送文件（UI 与队列） ======
  self.sendFiles=function(){
    var fi=document.getElementById('fileInput');
    if(!fi||!fi.files||fi.files.length===0){ alert('请选择文件'); return; }
    self.sendFilesFrom([].slice.call(fi.files)); fi.value='';
  };
  self.sendFilesFrom=function(files){
    var toConv = (self.activePeer==='all') ? 'all' : self.activePeer;
    var targets=[];
    if (toConv==='all'){ for(var k in self.conns){ if(self.conns[k].open) targets.push(k); } }
    else { if (self.conns[toConv] && self.conns[toConv].open) targets=[self.activePeer]; }
    if(!targets.length){ self.log('T40 FILE_SEND_BEGIN: no peers open'); alert('没有在线节点，无法发送文件'); return; }

    files.forEach(function(file){
      var ui = placeholder(file.name, file.size, true, toConv, self.myName);

      var localUrl = mkUrl(file);
      var m = normalizeMime(file.type, file.name);
      if (isImg(m, file.name)) {
        showImg(ui, localUrl);
      }
      else if (isVid(m, file.name) && canPlayVideo(m, file.name)){
        showVid(ui, localUrl, '发送中… (0/'+targets.length+')', null, file.name, null);
      }
      else if (isAudio(m, file.name) && canPlayAudio(m, file.name)){
        showAud(ui, localUrl, '发送中… (0/'+targets.length+')', file.name, null);
      }
      else {
        fileLink(ui, localUrl, file.name, file.size);
        updProg(ui, '发送中… (0/'+targets.length+')');
      }

      var gid = 'g'+Date.now()+'_'+Math.random().toString(36).slice(2,8);
      self._sendTrackers[gid] = { ui:ui, total:targets.length, done:0, name:file.name, size:file.size };

      function markDone(){
        var st = self._sendTrackers[gid]; if(!st) return;
        st.done++;
        if (st.done < st.total){
          updProg(st.ui, '发送中… ('+st.done+'/'+st.total+')');
        } else {
          updProg(st.ui, '已发送：'+st.name+' ('+human(st.size)+')');
        }
      }

      fileHashMeta(file).then(function(hash){
        try{ if (ui && ui.mediaEl) MediaCenter.attachHash(ui.mediaEl, hash||'', file.name||''); }catch(e){}
        self.log('SEND_QUEUE: '+file.name+' size='+file.size+' hash='+(hash||'')+' targets='+targets.length);
        targets.forEach(function(pid){
          enqueueFile(pid,file,hash,toConv,gid,markDone);
        });
      });
    });
  };

  function enqueueFile(pid,file,hash,toConv,gid,onOneDone){
    var st=self.conns[pid]; if(!st||!st.open){ self.log('对方不在线：'+shortId(pid)); onOneDone&&onOneDone(); return; }
    if(!st.queue) st.queue=[];
    var job={file:file,hash:hash,toConv:toConv,gid:gid,onOneDone:onOneDone};
    // 小文件优先（≤2MB插队到队列头）
    if((file&&file.size||0) <= 2*1024*1024) st.queue.unshift(job);
    else st.queue.push(job);
    if(!st.sending){ st.sending=true; sendNext(pid); }
  }
function sendNext(pid){
var st=self.conns[pid]; if(!st) return;
var job=st.queue && st.queue.shift();
if(!job){ st.sending=false; return; }
sendFileTo(pid, job.file, job.hash, job.toConv, function(success){
try{ job.onOneDone && job.onOneDone(success); }catch(e){}
sendNext(pid);
}, job.gid);
}
  // 功能开始：ensureBinaryConn（新增：确保 binary 连接）
  function ensureBinaryConn(pid, cb){
    var st=self.conns[pid]; if(!st) return cb(new Error('no-conn'));
    var cur = st.conn;
    var curSer = cur && (cur.serialization||'');
    self.log('BINCHK: pid='+pid+' open='+(!!(st.open&&cur&&cur.open))+' ser='+(curSer||'unknown'));
    if (cur && st.open && String(curSer)==='binary'){
      return cb(null, cur);
    }
    var nc;
    try{
      nc = self.peer.connect(pid,{reliable:true, serialization:'binary'});
      self.log('BINDIAL: dialing binary to '+pid);
    }
    catch(e){ self.log('BINDIAL_FAIL: '+(e.message||e)); return cb(e||new Error('dial-failed')); }
    try{ handleConn(nc,false); }catch(_){}
    var done=false;
    var timer=setTimeout(function(){
      if(done) return; done=true;
      try{ nc.close(); }catch(_e){}
      self.log('BINDIAL_TIMEOUT: '+pid);
      cb(new Error('dial-timeout'));
    }, Math.max(4000, DIAL_TIMEOUT_MS));
    nc.on('open', function(){
      if(done) return; done=true; clearTimeout(timer);
      var st2=self.conns[pid];
      if (st2 && st2.open && st2.conn===nc && String(nc.serialization||'')==='binary'){
        self.log('BINDIAL_OK: '+pid);
        cb(null, nc);
      }else{
        self.log('BINDIAL_NOT_ADOPTED: '+pid);
        cb(new Error('not-adopted'));
      }
    });
    nc.on('error', function(err){
      if(done) return; done=true; clearTimeout(timer);
      self.log('BINDIAL_ERROR: '+(err && (err.message||err.type)||err));
      cb(err||new Error('dial-error'));
    });
  }
  // 功能结束：ensureBinaryConn（新增）

  // 功能开始：sendFileTo发送文件（覆盖：发送前确保 binary 连接）
  function sendFileTo(pid,file,hash,toConv,done,gid){
    var st=self.conns[pid]; if(!st||!st.open){ self.log('对方不在线：'+shortId(pid)); return done&&done(false); }
    self.log('SEND_BEGIN: pid='+pid+' name='+file.name+' size='+file.size+' hash='+(hash||'')+' toConv='+toConv);

    ensureBinaryConn(pid, function(err,conn){
      if(err){ self.log('SEND_ABORT: ensureBinaryConn '+(err.message||err)); return done&&done(false); }

      var c=conn,
          id=String(Date.now())+'_'+Math.floor(Math.random()*1e6),
          chunk=self.chunkSize,
          state={off:0, reading:false},
          lastTs=0, lastPct=-1,
          mime=normalizeMime(file.type, file.name);

      st._files = st._files || {};
      st._files[id] = file;

      var posterP = Promise.resolve(null);
      if (isVid(mime, file.name)) {
        posterP = Promise.race([ extractVideoThumbnail(file), new Promise(function(r){ setTimeout(function(){ r(null); }, 3000); }) ]);
      }

      posterP.then(function(poster){
        try{
          c.send({type:'file-begin', id:id, name:file.name, size:file.size, mime:mime, chunk:chunk, hash:hash, poster:poster||null, to:(toConv==='all'?'all':pid)});
          self.log('SEND_META_OK: id='+id+' mime='+mime+' chunk='+chunk+' poster='+(!!poster));
        }catch(e){
          self.log('SEND_META_FAIL: '+(e.message||e));
          try{ delete st._files[id]; }catch(_){}
          return done&&done(false);
        }

        st._curSend = st._curSend || {};
        st._curSend[id] = {
          setOffset:function(n){ state.off = Math.max(0, Math.min(file.size, n|0)); self.log('SEND_RESUME_SET: id='+id+' offset='+state.off); }
        };

        var reader=new FileReader();
        reader.onerror=function(){
          self.log('SEND_READ_ERR: id='+id);
          try{ c.send({type:'file-end', id:id, hash:hash}); }catch(e){}
          try{ delete st._curSend[id]; }catch(_){}
          try{ delete st._files[id]; }catch(_){}
          done&&done(false);
        };
        reader.onload=function(e){
          flowSend(c,e.target.result,function(err2){
            if(err2){
              self.log('SEND_CHUNK_FAIL: id='+id+' off='+state.off+' err='+(err2.message||err2));
              try{ delete st._curSend[id]; }catch(_){}
              try{ delete st._files[id]; }catch(_){}
              return done&&done(false);
            }
            state.off += e.target.result.byteLength;
            var pct=Math.min(100,Math.floor(state.off*100/file.size));
            var nowTs=Date.now();
            if(pct!==lastPct && (nowTs-lastTs>300 || pct===100)){
              lastTs=nowTs; lastPct=pct;
              if (self._debug) self.log('SEND_PROGRESS: id='+id+' '+pct+'% ('+state.off+'/'+file.size+')');
            }
            if(state.off<file.size){ setTimeout(readNext,0); }
            else{
              try{ c.send({type:'file-end', id:id, hash:hash}); }catch(e){}
              try{ delete st._curSend[id]; }catch(_){}
              try{
                idbPutFull(hash||'', file, {name:file.name,size:file.size,mime:mime});
                self.fullSources[hash||'']=self.fullSources[hash||'']||new Set();
                self.fullSources[hash||''].add(self.localId);
              }catch(e){}
              try{ delete st._files[id]; }catch(_){}
              self.log('SEND_DONE: id='+id+' name='+file.name+' size='+file.size);
              done&&done(true);
            }
          });
        };
        function readNext(){
          var slice=file.slice(state.off,Math.min(state.off+chunk,file.size));
          try{ reader.readAsArrayBuffer(slice); }catch(e){ self.log('SEND_READ_EX: '+(e.message||e)); reader.onerror(); }
        }
        readNext();
      }).catch(function(){
        self.log('SEND_POSTER_FAIL');
        try{ delete st._files[id]; }catch(_){}
        done&&done(false);
      });
    });
  }
  // 功能结束：sendFileTo发送文件
  // 功能开始：连接处理（覆盖：去重采纳 + 优先采用 binary + 兼容分片）
  function handleConn(c,inbound){
    if(!c||!c.peer) return;
    var pid=c.peer;

    // DHT与限流初始化
    self._neigh = self._neigh || new Map();     // 邻居表 id -> ts
    self._targetPeers = self._targetPeers || 14;// 目标直连数
    self._maxPeers = self._maxPeers || 50;      // 硬上限
    self._seenOrder = self._seenOrder || [];    // 消息去重顺序（LRU修剪用）

    if(!self.conns[pid]) {
      self.conns[pid]={ conn:null, open:false, latency:0, sending:false, queue:[], recv:{cur:null,ui:null,convId:'all'}, _curSend:{}, _files:{} };
    }
    var st=self.conns[pid];

    // 采纳：优先binary；否则保留已打开；否则替换死连接
    var prev = st.conn;
    var prevSer = prev && (prev.serialization||'');
    var curSer  = c && (c.serialization||'');
    if (!prev) {
      st.conn = c;
      self.log('ADOPT_CONN: pid='+pid+' reason=first ser='+(curSer||'unknown'));
    } else if (prev !== c) {
      var prevOpen = !!(st.open && prev.open);
      var prevBin  = String(prevSer||'') === 'binary';
      var curBin   = String(curSer||'') === 'binary';
      if (curBin && !prevBin) {
        try{ prev.close(); }catch(e){}
        st.conn = c;
        self.log('ADOPT_CONN: pid='+pid+' reason=prefer-binary prevSer='+(prevSer||'unknown')+' curSer='+(curSer||'unknown'));
      } else if (prevOpen) {
        try{ c.close(); }catch(e){}
        self.log('DROP_DUP_CONN: pid='+pid+' reason=prev-open prevSer='+(prevSer||'unknown')+' curSer='+(curSer||'unknown'));
        return;
      } else {
        try{ prev.close(); }catch(e){}
        st.conn = c;
        self.log('ADOPT_CONN: pid='+pid+' reason=replace-dead prevSer='+(prevSer||'unknown')+' curSer='+(curSer||'unknown'));
      }
    }

    // 统一邻居拨号调度（轻量DHT）
    if(!self._scheduleDialNeighbors){
      self._scheduleDialNeighbors = function(ids){
        var now = Date.now();
        var list = Array.isArray(ids) ? ids : [];
        for(var i=0;i<list.length;i++){
          var id = list[i];
          if(!id || id===self.localId) continue;
          self._neigh.set(id, now);
        }
        // 达标则不再扩张
        var openCount=0, nonSeedOpen=0;
        for(var k in self.conns){
          if(self.conns[k].open){
            openCount++;
            if(!(self._seedIds && self._seedIds.has(k))) nonSeedOpen++;
          }
        }
        if (openCount >= self._targetPeers) return;

        // 选择候选并尝试拨号（最多3个）
        var back = self._dialBackoff || {};
        var candidates = Array.from(self._neigh.keys()).filter(function(id){
          if(id===self.localId) return false;
          if(self.conns[id] && self.conns[id].open) return false;
          if(self._dialing && self._dialing.has(id)) return false;
          var until = back[id]||0;
          return Date.now() >= until;
        }).sort(function(){ return Math.random()-0.5; }).slice(0, Math.min(3, self._targetPeers - openCount));
        candidates.forEach(function(id){ connectPeer(id); });
      };
    }

    c.on('open', function(){
      // 抢占：若当前连接未被采纳，且自己是binary而现有不是，则抢占；否则丢弃
      if (st.conn !== c){
        var cur = st.conn;
        var curBin = cur && String(cur.serialization||'')==='binary';
        var meBin  = String(c.serialization||'')==='binary';
        if (meBin && !curBin){
          try{ cur && cur.close(); }catch(e){}
          st.conn = c;
          self.log('ADOPT_CONN_ON_OPEN: pid='+pid+' reason=prefer-binary-at-open');
        }else{
          try{ c.close(); }catch(e){}
          self.log('DROP_DUP_ON_OPEN: pid='+pid);
          return;
        }
      }
      st.open=true;
      self.updateInfo();
      self.log((inbound?'入站':'出站')+'连接建立：'+pid+' ser='+(c.serialization||'unknown'));

      // 发送 hello + 邻居采样（最多20个）
      try{
        var neighSample = Object.keys(self.conns).filter(function(k){ return self.conns[k].open && k!==pid; })
          .sort(function(){ return Math.random()-0.5; }).slice(0,20);
        c.send({type:'hello', name: self.myName || ('用户-'+shortId(self.localId)), ver:2, neigh:neighSample, ts:Date.now()});
      }catch(e){ self.log('HELLO_SEND_ERR: '+(e.message||e)); }

      if (self._classic && self._classic.renderContacts){
        var arr=[]; for (var k in self.conns){ if(self.conns[k].open) arr.push({id:k,name: self.displayNames[k]||('节点 '+k.substring(0,8))}); }
        self._classic.renderContacts(arr, self.activePeer);
      }

      // 控制上限：连接数超过_maxPeers时优先断开seed
      try{
        var openIds = Object.keys(self.conns).filter(function(k){ return self.conns[k] && self.conns[k].open; });
        if(openIds.length > self._maxPeers){
          var seedsToClose = openIds.filter(function(k){ return self._seedIds && self._seedIds.has(k); });
          if(seedsToClose.length){
            var sid = seedsToClose[0];
            try{ self.conns[sid].conn.close(); self.log('MAX_PEERS_TRIM: close seed '+shortId(sid)); }catch(_){}
          }
        }
      }catch(_){}
    });

    c.on('data', function(d){
      if (st.conn !== c) return;
      try{
        // 二进制路径（保持原逻辑）
        var isAB = d && (d instanceof ArrayBuffer);
        var isBlob = (typeof Blob!=='undefined') && d instanceof Blob;
        var isTA = d && d.buffer && typeof d.byteLength === 'number' && Object.prototype.toString.call(d).indexOf('Array')!==-1;

        if (isAB || isBlob || isTA){
          var ctx = st.recv && st.recv.cur, ui = st.recv && st.recv.ui;
          if(!ctx || !ui) return;

          var part, sz;
          if (isAB){ part = new Uint8Array(d); sz = d.byteLength; }
          else if (isBlob){ part = d; sz = d.size || 0; }
          else { var u8 = new Uint8Array(d.buffer, d.byteOffset||0, d.byteLength); part = u8; sz = u8.byteLength; }

          ctx.parts.push(part);
          ctx.got += sz;

          var pct = ctx.size>0 ? Math.min(100, Math.floor(ctx.got*100/ctx.size)) : 0;

          if(!ctx.previewed){
            try{
              var outMime = normalizeMime(ctx.mime, ctx.name);
              var url=mkUrl(new Blob(ctx.parts,{type:outMime}));
              if (isImg(ctx.mime, ctx.name)){
                showImg(ui,url); ctx.previewed=true; ctx.previewUrl=url;
                self.log('PREVIEW_IMG: '+ctx.name+' got='+ctx.got+'/'+ctx.size);
              } else if (isVid(ctx.mime, ctx.name) && canPlayVideo(ctx.mime, ctx.name)){
                var need = Math.max(1, Math.floor((ctx.size||0) * self.previewPct / 100));
                if(ctx.got>=need){
                  showVid(ui,url,'可预览（接收中 '+pct+'%）', ctx.poster||null, ctx.name, ctx.hash||'');
                  ctx.previewed=true; ctx.previewUrl=url; ctx.mediaState={time:0, paused:true, kind:'video'};
                  self.log('PREVIEW_VIDEO: '+ctx.name+' need='+need+' got='+ctx.got);
                }
              } else if (isAudio(ctx.mime, ctx.name) && canPlayAudio(ctx.mime, ctx.name)){
                var needA = Math.max(1, Math.floor((ctx.size||0) * self.previewPct / 100));
                if(ctx.got>=needA){
                  showAud(ui,url,'可预览（接收中 '+pct+'%）', ctx.name, ctx.hash||'');
                  ctx.previewed=true; ctx.previewUrl=url; ctx.mediaState={time:0, paused:true, kind:'audio'};
                  self.log('PREVIEW_AUDIO: '+ctx.name+' need='+needA+' got='+ctx.got);
                }
              }
            }catch(e){}
          }

          try{ if (ui && ui.progress){ ui.progress.textContent = '接收中 '+pct+'%'; } }catch(e){}

          try{
            if (ctx.hash && (ctx.got - (ctx.lastSaved||0) >= PART_FLUSH)){
              idbPutPart(ctx.hash, {got:ctx.got, size:ctx.size, name:ctx.name, mime:ctx.mime});
              ctx.lastSaved = ctx.got;
              if (self._debug) self.log('PART_FLUSH: '+ctx.name+' got='+ctx.got);
            }
          }catch(e){}
          return;
        }

        // 信令消息
        if (!d || typeof d!=='object') return;
        switch(d.type){
          case 'hello':
            self.displayNames[pid] = String(d.name||'') || ('节点 '+shortId(pid));
            if (self._classic && self._classic.renderContacts){
              var arr=[]; for (var k in self.conns){ if(self.conns[k].open) arr.push({id:k,name: self.displayNames[k]||('节点 '+k.substring(0,8))}); }
              self._classic.renderContacts(arr, self.activePeer);
            }
            // 记录邻居并按需扩展
            if (d.neigh && Array.isArray(d.neigh)) self._scheduleDialNeighbors(d.neigh);

            // 若这是seed且已拥有足够非seed直连，温和减负（避免长期依赖seed）
            try{
              var isSeed = self._seedIds && self._seedIds.has(pid);
              if (isSeed){
                var openIds = Object.keys(self.conns).filter(function(k){ return self.conns[k].open; });
                var nonSeed = openIds.filter(function(k){ return !(self._seedIds && self._seedIds.has(k)); });
                if (nonSeed.length >= Math.max(6, self._targetPeers-2)){
                  setTimeout(function(){
                    if (self.conns[pid] && self.conns[pid].open){
                      try{ self.conns[pid].conn.close(); self.log('SEED_RELAX: close '+shortId(pid)); }catch(_){}
                    }
                  }, 800);
                }
              }
            }catch(_){}
            break;

          case 'chat':
            if (d && d.mid){
              if (self._seenMsgIds.has(d.mid)) return;
              self._seenMsgIds.add(d.mid);
              self._seenOrder.push(d.mid);
            }
            pushChat(String(d.text||''), false, (d.to==='all'?'all':pid), self.displayNames[pid] || ('节点 '+shortId(pid)));
            // 轻量DHT群聊转发（仅群聊，防环路：mid去重 + relay标记）
            if (d && d.to==='all' && !d.relay){
              var fwd = {
                type:'chat',
                text: String(d.text||''),
                to:'all',
                from: d.from || pid,
                mid: d.mid || (String(Date.now())+'_'+Math.random().toString(36).slice(2,8)),
                relay: true
              };
              for (var kk in self.conns){
                if (!self.conns[kk] || !self.conns[kk].open) continue;
                if (kk === pid) continue;
                try{ self.conns[kk].conn.send(fwd); }catch(e){}
              }
            }
            break;

          case 'ping':
            try{ c.send({type:'pong',ts:d.ts||Date.now()}); }catch(e){}
            break;

          case 'pong':
            st.latency = Date.now() - Number(d.ts||Date.now());
            if (self._debug) self.log('PING_RTT: pid='+pid+' rtt='+st.latency+'ms');
            break;

          case 'file-begin':
            handleFileBegin(pid, c, d, false);
            break;

          case 'file-resume':
            try{
              if (st._curSend && st._curSend[d.id] && typeof st._curSend[d.id].setOffset==='function'){
                st._curSend[d.id].setOffset(Number(d.offset||0));
              }
            }catch(e){}
            break;

          case 'file-end':
            finalizeReceive(pid, d.id, d.hash);
            break;

          case 'file-has':
            if (d.hash){
              self.fullSources[d.hash]=self.fullSources[d.hash]||new Set();
              self.fullSources[d.hash].add(pid);
            }
            break;

          case 'stream-open':
            try{ self.openStreamForFile(pid, d); }catch(e){}
            break;

          case 'stream-opened':
            try{ var ui = self._streamUiByFid[d.fid]; if (ui) updProg(ui, '直播中…'); }catch(e){}
            break;

          case 'stream-error':
            try{ var ui2 = self._streamUiByFid[d.fid]; if (ui2) updProg(ui2, '直播错误'); }catch(e){}
            break;

          default:
            break;
        }
      }catch(e){}
    });

    c.on('close', function(){
      if (st.conn === c) {
        st.open=false;
        self.updateInfo();
        self.log('连接关闭：'+pid);
        try{ if (st.recv && st.recv.cur && st.recv.ui){ updProg(st.recv.ui,'连接中断'); } }catch(e){}
      } else {
        self.log('DROP_OLD_CLOSE: pid='+pid);
      }
    });

    c.on('error', function(err){
      if (st.conn === c) self.log('对端错误：'+(err && (err.message||err.type)||err));
    });
  }
  // 功能结束：连接处理（覆盖：去重采纳 + 优先采用 binary + 兼容分片）

  // ====== 定时器 & 断开/通话 ======
  function startTimers(){
    stopTimers();

    self.timers.up=setInterval(function(){
      if(!self.isConnected||!self.startAt) return;
      var s=Math.floor((Date.now()-self.startAt)/1000),
          h=Math.floor(s/3600),
          m=Math.floor((s%3600)/60),
          sec=s%60;
      var t=(h<10?'0':'')+h+':'+(m<10?'0':'')+m+':'+(sec<10?'0':'')+sec;
      var up=document.getElementById('uptime'); if(up) up.textContent=t;
      if (typeof window.updateEntryStatus === 'function'){
        window.updateEntryStatus({
          connected:true,
          online:Object.keys(self.conns).filter(function(k){return self.conns[k].open;}).length,
          localId:self.localId, virtualIp:self.virtualIp, uptime:t
        });
      }
    },1000);

    self.timers.ping=setInterval(function(){
      for(var k in self.conns){
        var st=self.conns[k]; if(!st.open) continue;
        try{ st.conn.send({type:'ping',ts:Date.now()}); }catch(e){}
      }
    },5000);

    // 维护计时器：邻居修剪/补连、消息去重LRU、seed减负
    self.timers.maint=setInterval(function(){
      try{
        // 修剪邻居 >90s
        self._neigh = self._neigh || new Map();
        var now=Date.now(), ttl=90000;
        self._neigh.forEach(function(ts,id){ if(now - ts > ttl) self._neigh.delete(id); });

        // 不足目标直连数时，从邻居补连
        var openIds = Object.keys(self.conns).filter(function(k){ return self.conns[k] && self.conns[k].open; });
        if (openIds.length < (self._targetPeers || 14)){
          var ids = Array.from(self._neigh.keys());
          if (self._scheduleDialNeighbors) self._scheduleDialNeighbors(ids);
        }

        // 消息去重LRU：保留最近5000条
        self._seenMsgIds = self._seenMsgIds || new Set();
        self._seenOrder = self._seenOrder || [];
        var limit=5000;
        if (self._seenOrder.length > limit){
          var drop = self._seenOrder.length - limit;
          while (drop-- > 0){
            var mid = self._seenOrder.shift();
            if (mid) self._seenMsgIds.delete(mid);
          }
        }

        // seed减负：若非seed直连达到阈值，关闭多余seed
        var seedsOpen = openIds.filter(function(k){ return self._seedIds && self._seedIds.has(k); });
        var nonSeedOpen = openIds.filter(function(k){ return !(self._seedIds && self._seedIds.has(k)); });
        if (seedsOpen.length > 1 && nonSeedOpen.length >= Math.max(6, (self._targetPeers||14)-2)){
          var sid = seedsOpen[0];
          try{ self.conns[sid].conn.close(); self.log('SEED_TRIM: close '+shortId(sid)); }catch(_){}
        }
      }catch(_){}
    }, 15000);
  }
  function stopTimers(){
    if(self.timers.up){clearInterval(self.timers.up); self.timers.up=null;}
    if(self.timers.ping){clearInterval(self.timers.ping); self.timers.ping=null;}
    if(self.timers.maint){clearInterval(self.timers.maint); self.timers.maint=null;}
  }
  self.disconnect=function(){
    for(var k in self.conns){ try{ self.conns[k].conn.close(); }catch(e){} }
    self.conns={}; self.fullSources={};
    if(self.peer){ try{ self.peer.destroy(); }catch(e){} self.peer=null; }
    self.isConnected=false; self.startAt=0; self.localId=''; self.virtualIp='';
    setStatus('离线'); self.updateInfo();
    stopTimers();
    self.log('已断开');
  };
  self.quickCall=function(){
    if (!self.peer || !self.isConnected){ alert('未连接'); return; }
    var open = Object.keys(self.conns).filter(function(k){ return self.conns[k] && self.conns[k].open; });
    if (!open.length){ alert('没有在线对象'); return; }
    var pid = (self.activePeer && self.activePeer!=='all' && self.conns[self.activePeer] && self.conns[self.activePeer].open)
      ? self.activePeer
      : (open.length===1 ? open[0] : null);
    if (!pid && open.length>1){
      var names = open.map(function(k,i){ return (i+1)+'. '+(self.displayNames[k]||('节点 '+k.slice(0,8))); }).join('\n');
      var ans = prompt('选择视频通话对象：输入序号\n'+names, '1');
      var idx = parseInt(ans||'',10);
      if (idx>=1 && idx<=open.length) pid = open[idx-1];
    }
    if (!pid){ return; }
    self.activePeer = pid;
    self.toggleCall(false);
  };
  self.toggleCall=function(forceClose){
    if (!window.__ENTRY_PAGE__) return;
    self._media = self._media || {};
    if (self._media.call || forceClose){
      try{ self._media.call && self._media.call.close(); }catch(e){}
      if (self._media.local){ try{ self._media.local.getTracks().forEach(function(t){t.stop();}); }catch(e){} }
      self._media.call=null; self._media.local=null;
      var rv=document.getElementById('remoteVideo'); if(rv) rv.srcObject=null;
      var lv=document.getElementById('localVideo');  if(lv) lv.srcObject=null;
      return;
    }
    var pid=self.activePeer;
    if(!pid || pid==='all'){ alert('请先选择通话对象'); return; }
    if(!self.peer){ alert('未连接'); return; }
    navigator.mediaDevices.getUserMedia({video:true,audio:true}).then(function(stream){
      self._media.local=stream;
      var lv=document.getElementById('localVideo'); if(lv) lv.srcObject=stream;
      var call=self.peer.call(pid, stream);
      self._media.call=call;
      call.on('stream', function(remote){ var rv=document.getElementById('remoteVideo'); if(rv) rv.srcObject=remote; });
      call.on('close', function(){ self.toggleCall(true); });
      call.on('error', function(){ self.toggleCall(true); });
    }).catch(function(){ alert('无法获取摄像头/麦克风'); });
  };

  // ====== 发送端：收到流式请求，开启直播（严格分离，不做回退） ======
  self.openStreamForFile = function(pid, req){
    var st = self.conns[pid];
    if(!st) return;

    var sid = 's'+Date.now()+'_'+Math.random().toString(36).slice(2,6);
    if(self._streams[sid]){
      self.log('直播已存在：'+sid);
      return;
    }

    st._files = st._files || {};
    var file = st._files[req.fid];
    if (!file){
      try{
        st.conn.send({
          type:'stream-error', fid:req.fid, reason:'not-found',
          name: req.name || '', mime: req.mime || '', size: 0, poster: req.poster || null
        });
      }catch(e){}
      return;
    }

    var isAudio = (req.media === 'audio');
    var media = isAudio ? document.createElement('audio') : document.createElement('video');
    media.preload = 'auto';
    media.playsInline = true;
    media.muted = true;
    media.volume = 0;
    media.src = URL.createObjectURL(file);

    var getStream = media.captureStream || media.mozCaptureStream || media.webkitCaptureStream;
    if (!getStream){
      try{
        st.conn.send({
          type:'stream-error', fid:req.fid, reason:'no-captureStream',
          name: file.name || '', mime: (file.type||''), size: file.size||0, poster: req.poster || null
        });
      }catch(e){}
      cleanupMedia();
      return;
    }

    var stream;
    try{
      stream = getStream.call(media);
    }catch(e){
      try{
        st.conn.send({
          type:'stream-error', fid:req.fid, reason:'capture-failed',
          name: file.name || '', mime: (file.type||''), size: file.size||0, poster: req.poster || null
        });
      }catch(_){}
      cleanupMedia();
      return;
    }

    var call;
    try{
      call = self.peer.call(pid, stream, {
        metadata: {
          kind:'file-stream',
          sid:sid,
          fid:req.fid,
          name:req.name || file.name || '',
          poster:req.poster || null,
          media:req.media || (isAudio?'audio':'video')
        }
      });
      self.log('STREAM_CALL_OUT: pid='+pid+' sid='+sid);
    }catch(e){
      try{
        st.conn.send({
          type:'stream-error', fid:req.fid, reason:'call-failed',
          name: file.name || '', mime: (file.type||''), size: file.size||0, poster: req.poster || null
        });
      }catch(_){}
      cleanupMedia();
      return;
    }

    self._streams[sid] = { call:call, pid:pid, fid:req.fid, kind:(isAudio?'audio':'video'), localEl:media };

    media.play().catch(function(){});

    call.on('close', function(){
      cleanupMedia();
      delete self._streams[sid];
      self.log('STREAM_CLOSE: sid='+sid);
    });
    call.on('error', function(){
      try{
        st.conn.send({
          type:'stream-error', fid:req.fid, reason:'call-error',
          name: file.name || '', mime: (file.type||''), size: file.size||0, poster: req.poster || null
        });
      }catch(_){}
      try{ call.close(); }catch(_){}
      cleanupMedia();
      delete self._streams[sid];
      self.log('STREAM_ERROR: sid='+sid);
    });

    try{ st.conn.send({type:'stream-opened', fid:req.fid, sid:sid}); }catch(_){}

    function cleanupMedia(){
      try{ media.pause(); }catch(_){}
      try{
        var u = media.src;
        media.src = '';
        if(u && u.startsWith('blob:')) URL.revokeObjectURL(u);
      }catch(_){}
    }
  };

  return self;
})();

// ====== 接收端：初始化/完成接收 ======
function handleFileBegin(pid, c, d, forceDownload){
  try{
    var h=d.hash||'';
    var convId = (d.to==='all') ? 'all' : pid;
    var appRef = window.app;

    // 严格模式：在线播放时不走直传（音/视频），主动请求直播
    if(!forceDownload && appRef.streamMode && (isVid(d.mime, d.name) || isAudio(d.mime, d.name))){
      var ui = appRef._streamUiByFid[d.id];
      if (!ui){
        ui = (appRef._classic && appRef._classic.placeholderWithConv)
          ? appRef._classic.placeholderWithConv(d.name||'媒体', d.size||0, false, convId, appRef.displayNames[pid] || ('节点 '+shortId(pid)))
          : null;
        appRef._streamUiByFid[d.id] = ui;
      }else{
        try{ (appRef._classic && appRef._classic.updateProgressText) && appRef._classic.updateProgressText(ui, '准备直播…'); }catch(e){}
      }
      try{
        c.send({
          type:'stream-open',
          fid:d.id, name:d.name||'', mime:d.mime||'', size:d.size||0,
          poster:d.poster||null, media: isAudio(d.mime,d.name)?'audio':'video'
        });
      }catch(e){}
      appRef.log('STREAM_REQ: from='+pid+' name='+d.name+' size='+(d.size||0));
      return;
    }

    // 直传路径：命中完整缓存直接完成
    if(h){
      idbGetFull(h, function(rec){
        if(rec && rec.blob){
          var url=(appRef._classic && appRef._classic.mkUrl) ? appRef._classic.mkUrl(rec.blob) : URL.createObjectURL(rec.blob);
          var m=(rec.meta&&rec.meta.mime)||'';
          var n=(rec.meta&&rec.meta.name)||d.name||'文件';
          var ui=(appRef._classic && appRef._classic.placeholderWithConv)
            ? appRef._classic.placeholderWithConv(n, (rec.meta&&rec.meta.size)||d.size||0, false, convId, appRef.displayNames[pid] || ('节点 '+shortId(pid)))
            : null;
          if (isImg(m,n) && appRef._classic && appRef._classic.showImage) appRef._classic.showImage(ui,url);
          else if (isVid(m,n) && canPlayVideo(m,n) && appRef._classic && appRef._classic.showVideo) appRef._classic.showVideo(ui,url,'本地缓存', d.poster||null, n, h);
          else if (isAudio(m,n) && canPlayAudio(m,n) && appRef._classic && appRef._classic.showAudio) appRef._classic.showAudio(ui,url,'本地缓存', n, h);
          else if (appRef._classic && appRef._classic.showFileLink) appRef._classic.showFileLink(ui,url,n,(rec.meta&&rec.meta.size)||d.size||0);
          try{ c.send({type:'file-end',id:d.id,hash:h}); }catch(e){}
          appRef.log('CACHE_HIT: '+n+' size='+(rec.meta&&rec.meta.size||0));
          return;
        }

        setupRecv();
        idbGetPart(h, function(rec2){
          if(rec2 && rec2.meta && typeof rec2.meta.got==='number' && rec2.meta.got<(d.size||0)){
            var off = rec2.meta.got|0;
            try{ c.send({type:'file-resume', id:d.id, hash:h, offset:off}); }catch(e){}
            appRef.log('RESUME_REQ: id='+d.id+' hash='+h+' off='+off+' size='+(d.size||0));
          }
        });
      });
      return;
    }

    setupRecv();

    function setupRecv(){
      var ui=(appRef._classic && appRef._classic.placeholderWithConv)
        ? appRef._classic.placeholderWithConv(d.name||'文件', d.size||0, false, convId, appRef.displayNames[pid] || ('节点 '+shortId(pid)))
        : null;
      if (!appRef.conns[pid]) appRef.conns[pid]={};
      appRef.conns[pid].recv = appRef.conns[pid].recv || {};
      appRef.conns[pid].recv.cur={
        id:d.id, name:d.name, size:d.size||0, mime:normalizeMime(d.mime,d.name),
        got:0, parts:[], previewed:false, previewUrl:null, mediaState:null, hash:h, poster:d.poster||null, lastSaved:0, convId: convId
      };
      appRef.conns[pid].recv.ui=ui;
      appRef.conns[pid].recv.convId=convId;
      appRef.log('RECV_INIT: from='+pid+' name='+d.name+' size='+(d.size||0)+' hash='+(h||''));
    }
  }catch(e){
    try{ window.app && window.app.log('RECV_BEGIN_EX: '+(e.message||e)); }catch(_) {}
  }
}

function finalizeReceive(pid,id,hash){
  try{
    var appRef = window.app;
    var st=appRef.conns[pid]; if(!st||!st.recv) return;
    var ctx=st.recv.cur, ui=st.recv.ui;
    if(!ctx||ctx.id!==id) return;

    var outMime = normalizeMime(ctx.mime, ctx.name);
    var blob=new Blob(ctx.parts,{type:outMime});
    var newUrl=(appRef._classic && appRef._classic.mkUrl) ? appRef._classic.mkUrl(blob) : URL.createObjectURL(blob);

    if (isImg(ctx.mime, ctx.name)){
      appRef._classic && appRef._classic.showImage && appRef._classic.showImage(ui,newUrl);
      appRef._classic && appRef._classic.updateProgressText && appRef._classic.updateProgressText(ui,'接收完成');
    }
    else if (isVid(ctx.mime, ctx.name) && canPlayVideo(ctx.mime, ctx.name)){
      appRef._classic && appRef._classic.showVideo && appRef._classic.showVideo(ui, newUrl, '接收完成', ctx.poster||null, ctx.name, ctx.hash||hash||'');
    }
    else if (isAudio(ctx.mime, ctx.name) && canPlayAudio(ctx.mime, ctx.name)){
      appRef._classic && appRef._classic.showAudio && appRef._classic.showAudio(ui, newUrl, '接收完成', ctx.name, ctx.hash||hash||'');
    }
    else{
      appRef._classic && appRef._classic.showFileLink && appRef._classic.showFileLink(ui,newUrl,ctx.name,ctx.size);
      appRef._classic && appRef._classic.updateProgressText && appRef._classic.updateProgressText(ui,'接收完成：'+ctx.name+' ('+human(ctx.size)+')');
    }

    if (ctx.size && blob.size !== ctx.size){
      appRef._classic && appRef._classic.updateProgressText && appRef._classic.updateProgressText(ui, '接收完成（大小异常：'+human(blob.size)+' / '+human(ctx.size)+'）');
      appRef.log('SIZE_MISMATCH id='+id+' got='+blob.size+' expect='+ctx.size);
    }

    try{
      if(ctx.previewUrl && ctx.previewUrl !== newUrl) URL.revokeObjectURL(ctx.previewUrl);
    }catch(e){}

    try{
      idbPutFull(hash||ctx.hash||'', blob, {name:ctx.name,size:ctx.size,mime:outMime});
      if (ctx.hash) idbDelPart(ctx.hash);
      appRef.fullSources[hash||ctx.hash||'']=appRef.fullSources[hash||ctx.hash||'']||new Set();
      appRef.fullSources[hash||ctx.hash||''].add(appRef.localId);
      for(var k in appRef.conns){
        var s=appRef.conns[k]; if(s.open){ try{ s.conn.send({type:'file-has', hash:(hash||ctx.hash||'')}); }catch(e){} }
      }
      appRef.log('IDB_PUT: '+ctx.name+' size='+ctx.size+' hash='+(hash||ctx.hash||''));
    }catch(e){
      appRef.log('IDB_PUT_FAIL: '+(e.message||e));
    }

    st.recv.cur=null; st.recv.ui=null;
    appRef.log('RECV_DONE: '+ctx.name+' '+human(ctx.size));

    try{
      var activeConv = (appRef._classic && appRef._classic.getConvEl) ? appRef._classic.getConvEl(ctx.convId) : document.getElementById('msgScroll');
      if(activeConv){ activeConv.scrollTop=activeConv.scrollHeight; }
    }catch(e){}
    try{ idbCleanupStaleParts(); }catch(e){}
  }catch(e){
    try{ window.app && window.app.log('RECV_FINALIZE_EX: '+(e.message||e)); }catch(_) {}
  }
}

// ===================== 经典 UI 绑定（含详细日志） =====================
function bindClassicUI(app){
  if (!window.CLASSIC_UI) { try{ app.log('UI_SKIP: CLASSIC_UI=false'); }catch(e){} return; }
  if (app.__uiBound) return;
  app.__uiBound = true;

  var editor = document.getElementById('editor');
  var sendBtn = document.getElementById('sendBtn');
  var fileInput = document.getElementById('fileInput');
  var messages = document.querySelector('.messages');
  var msgScroll = document.getElementById('msgScroll');
  var contactList = document.getElementById('contactList');
  var contactSearch = document.getElementById('contactSearch');
  var sendArea = document.getElementById('sendArea');
  var statusChip = document.getElementById('statusChip');
  var onlineChip = document.getElementById('onlineChip');
  var appRoot = document.querySelector('.app') || document.body;
  app.uiRoot = appRoot;
  try{ app.log('UI_BIND: ok'); }catch(e){}

  function textOfEditor(){
    if (!editor) return '';
    var t = editor.innerText || editor.textContent || '';
    return t.replace(/\u00A0/g,' ').replace(/\r/g,'').trim();
  }
  function clearEditor(){ if(editor){ editor.innerHTML=''; editor.textContent=''; } }
  function syncSendBtn(){
    if (!sendBtn) return;
    var hasText = textOfEditor().length>0;
    sendBtn.disabled = !(app && app.isConnected && hasText);
  }

  // 复制辅助
  function attachCopyHandlers(el, getText){
    if (!el) return;
    var timer=null, down=false, threshold=600;
    function clearTimer(){ if(timer){ clearTimeout(timer); timer=null; } }
    function startPress(){ down=true; clearTimer(); timer=setTimeout(function(){ if(down){ doCopy(); } }, threshold); }
    function endPress(){ down=false; clearTimer(); }
    function doCopy(){
      try{
        var txt = (typeof getText==='function') ? String(getText()||'') : (el.innerText||'');
        if (!txt) return;
        if (navigator.clipboard && navigator.clipboard.writeText){
          navigator.clipboard.writeText(txt).then(function(){
            try{
              el.setAttribute('data-copied','已复制');
              setTimeout(function(){ el.removeAttribute('data-copied'); },800);
            }catch(e){}
          });
        }else{
          var ta=document.createElement('textarea');
          ta.value=txt; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta);
        }
      }catch(e){}
    }
    el.addEventListener('pointerdown', startPress);
    el.addEventListener('pointerup', endPress);
    el.addEventListener('pointerleave', endPress);
    el.addEventListener('dblclick', function(e){ e.preventDefault(); doCopy(); });
    el.style.userSelect = 'text';
  }

  app._classic = {
    mkUrl: function(blob){ return URL.createObjectURL(blob); },
    appendChatWithConv: function(text, mine, convId, senderName){
      try{
        var convEl = this.getConvEl ? this.getConvEl(convId||'all') : msgScroll;
        if (!convEl) return;
        var row=document.createElement('div'); row.className='row'+(mine?' right':'');
        var av=document.createElement('div'); av.className='avatar-sm';
        var lt=document.createElement('span'); lt.className='letter'; lt.textContent = initialChar(senderName, !!mine);
        av.appendChild(lt);
        var bubble=document.createElement('div'); bubble.className='bubble'+(mine?' me':''); bubble.textContent=String(text||'');
        if (mine){ row.appendChild(bubble); row.appendChild(av); } else { row.appendChild(av); row.appendChild(bubble); }
        convEl.appendChild(row); convEl.scrollTop = convEl.scrollHeight;
        attachCopyHandlers(bubble, function(){ return String(text||''); });
      }catch(e){ try{ app.log('UI_CHAT_EX: '+(e.message||e)); }catch(_){} }
    },
    placeholderWithConv: function(name,size,mine,convId, senderName){
      try{
        var convEl = this.getConvEl ? this.getConvEl(convId||'all') : msgScroll;
        if (!convEl) return null;
        var row=document.createElement('div'); row.className='row'+(mine?' right':'');
        var av=document.createElement('div'); av.className='avatar-sm';
        var lt=document.createElement('span'); lt.className='letter'; lt.textContent = initialChar(senderName, !!mine);
        av.appendChild(lt);
        var bubble=document.createElement('div'); bubble.className='bubble file'+(mine?' me':'');
        var safe = String(name||'文件').replace(/"/g,'"');
        bubble.innerHTML = '<div class="file-link"><div class="file-info"><span class="file-icon">📄</span>'
          + '<span class="file-name" title="'+safe+'">'+safe+'</span></div>'
          + '<div class="progress-line">'+(mine?'准备发送…':'准备接收…')+'</div></div>';
        if (mine){ row.appendChild(bubble); row.appendChild(av); } else { row.appendChild(av); row.appendChild(bubble); }
        convEl.appendChild(row); convEl.scrollTop=convEl.scrollHeight;
        var ui = {root:row, progress:bubble.querySelector('.progress-line'), mediaWrap:bubble, convId: convId||'all'};
        attachCopyHandlers(bubble, function(){ return safe; });
        return ui;
      }catch(e){ try{ app.log('UI_PH_EX: '+(e.message||e)); }catch(_){}; return null; }
    },
    showImage: function(ui,url){
      if(!ui||!ui.mediaWrap) return;
      ui.mediaWrap.classList.add('media');
      ui.mediaWrap.innerHTML = '<a href="'+url+'" target="_blank" rel="noopener" class="thumb-link">'
        + '<img class="thumb img" src="'+url+'"></a>';
      ui.progress = ui.mediaWrap.querySelector('.progress-line') || ui.progress;
      attachCopyHandlers(ui.mediaWrap, function(){ return '图片 '+url; });
    },
    showVideo: function(ui,url,info,poster,name,hash){
      if(!ui||!ui.mediaWrap) return;
      ui.mediaWrap.classList.add('media');
      var posterAttr = poster ? ' poster="'+poster+'"' : '';
      ui.mediaWrap.innerHTML =
        '<video controls preload="metadata" src="'+url+'"'+posterAttr+' style="width:var(--thumb);border-radius:8px;background:#000"></video>'
        + '<div class="progress-line">'+(info?String(info):'')+'</div>';
      var v = ui.mediaWrap.querySelector('video');
      var note = ui.mediaWrap.querySelector('.progress-line');
      ui.mediaEl = v;
      ui.progress = note;
      try{ MediaCenter.register(v, {hash:hash||'', name:name||''}); }catch(e){}
      attachCopyHandlers(ui.mediaWrap, function(){ return (name||'视频')+' '+url; });
    },
    showAudio: function(ui,url,info,name,hash){
      if(!ui||!ui.mediaWrap) return null;
      ui.mediaWrap.classList.add('media');
      var safe = String(name||'音乐').replace(/"/g,'"');
      ui.mediaWrap.innerHTML =
        '<div class="qq-audio">'
        +  '<button class="disc" title="播放/暂停">♪</button>'
        +  '<span class="title" title="'+safe+'">'+safe+'</span>'
        +  '<span class="info">'+(info?String(info):'')+'</span>'
        +  '<audio class="audio-el" preload="metadata" src="'+url+'" style="display:none"></audio>'
        + '</div>';
      var wrap = ui.mediaWrap.querySelector('.qq-audio');
      var disc = ui.mediaWrap.querySelector('.disc');
      var infoEl = ui.mediaWrap.querySelector('.info');
      var audio = ui.mediaWrap.querySelector('.audio-el');
      MediaCenter.register(audio, {hash:hash||'', name:name||'', playBtn:disc, noteEl:infoEl, btnStyle:'disc'});
      wrap.addEventListener('click', function(e){
        if (e.target === disc) return;
        try{ if (audio.paused) audio.play().catch(function(){}); else audio.pause(); }catch(er){}
      });
      ui.mediaEl = audio;
      ui.progress = infoEl;
      attachCopyHandlers(ui.mediaWrap, function(){ return safe+' '+url; });
      return {audio:audio, disc:disc, info:infoEl};
    },
    showFileLink: function(ui,url,name,size){
      if(!ui||!ui.mediaWrap) return;
      var safe=String(name||'文件').replace(/"/g,'"');
      ui.mediaWrap.classList.remove('media');
      ui.mediaWrap.innerHTML = '<a href="'+url+'" target="_blank" rel="noopener" class="file-link" title="'+safe+'">'
        + '<div class="file-info"><span class="file-icon">📄</span><span class="file-name">'+safe+'</span></div>'
        + '<div class="progress-line">下载：'+safe+' ('+human(size||0)+')</div></a>';
      ui.progress = ui.mediaWrap.querySelector('.progress-line') || ui.progress;
      attachCopyHandlers(ui.mediaWrap, function(){ return safe+' '+url; });
    },
    updateProgressText: function(ui,txt){ if(ui&&ui.progress) ui.progress.textContent = String(txt||''); },
    updateStatus: function(){
      if (statusChip) statusChip.textContent = app.isConnected ? '已连接' : '未连接';
      if (onlineChip){
        var openCount=0; for (var k in app.conns){ if(app.conns[k].open) openCount++; }
        onlineChip.textContent = '在线 ' + openCount;
      }
      syncSendBtn();
    },
    getEditorText: textOfEditor,
    clearEditor: clearEditor,
    // 会话容器
    getConvEl: (function(){
      var messages = document.querySelector('.messages');
      var convMap = {}; var activeConvId = app.activePeer || 'all';
      var base = document.getElementById('msgScroll');
      if (base){ convMap['all'] = base; }
      function ensureConv(id){
        id = id || 'all';
        if (convMap[id]) return convMap[id];
        var el = document.createElement('div');
        el.className = 'msg-scroll';
        el.style.display = (id==='all'?'block':'none');
        if (messages) messages.appendChild(el);
        convMap[id] = el;
        return el;
      }
      return function(id){ id=id||activeConvId||'all'; return ensureConv(id); };
    })(),
    renderContacts: function(list, activeId){
      if (!contactList) return;
      var kw = (contactSearch && contactSearch.value || '').trim().toLowerCase();
      contactList.innerHTML='';
      var all=document.createElement('div'); all.className='contact'+((activeId==='all')?' active':''); all.dataset.id='all';
      all.innerHTML='<div class="avatar"></div><div><div class="cname">所有人（群聊）</div><div class="cmsg">公开群聊</div></div>';
        all.addEventListener('click', function(){
          app.activePeer='all';
          contactList.querySelectorAll('.contact.active').forEach(function(el){ el.classList.remove('active'); });
          all.classList.add('active');
          var msgs=document.querySelector('.messages');
          if(msgs){ msgs.querySelectorAll('.msg-scroll').forEach(function(el){ el.style.display='none'; }); var el=document.getElementById('msgScroll'); if(el) el.style.display='block'; }
        });
      contactList.appendChild(all);
      for (var pid in app.conns){
        if (!app.conns.hasOwnProperty(pid)) continue;
        if (!app.conns[pid].open) continue;
        var nm = app.displayNames[pid] || ('节点 '+pid.substring(0,8));
        if (kw && nm.toLowerCase().indexOf(kw)===-1) continue;
        var row=document.createElement('div'); row.className='contact'+((activeId===pid)?' active':''); row.dataset.id=pid;
        row.innerHTML='<div class="avatar"></div><div><div class="cname"></div><div class="cmsg">在线</div></div>';
        row.querySelector('.cname').textContent = nm;
        row.addEventListener('click', function(){
          var id = this.dataset.id;
          app.activePeer = id;
          contactList.querySelectorAll('.contact.active').forEach(function(el){ el.classList.remove('active'); });
          this.classList.add('active');
          var msgs=document.querySelector('.messages');
          if(msgs){
            msgs.querySelectorAll('.msg-scroll').forEach(function(el){ el.style.display='none'; });
            var el = app._classic.getConvEl ? app._classic.getConvEl(id) : null;
            if(el) el.style.display='block';
          }
        });
        contactList.appendChild(row);
      }
      try{ app.log('UI_CONTACTS: rendered'); }catch(e){}
    }
  };

  // 编辑器交互
  if (editor){
    editor.addEventListener('input', syncSendBtn);
    var composing=false;
    editor.addEventListener('compositionstart', function(){ composing=true; });
    editor.addEventListener('compositionend', function(){ composing=false; });
    editor.addEventListener('keydown', function(e){
      if (e.key==='Enter' && !e.shiftKey && !composing){ e.preventDefault(); app.sendMsg(); }
    });
  }
  var emojiBtn=document.getElementById('emojiBtn');
  if (emojiBtn && editor){
    emojiBtn.addEventListener('click', function(){
      editor.focus();
      try{ document.execCommand('insertText', false, '😀'); }catch(e){
        var r=document.createRange(); r.selectNodeContents(editor); r.collapse(false);
        var s=window.getSelection(); s.removeAllRanges(); s.addRange(r);
        var node=document.createTextNode('😀'); r.insertNode(node);
      }
      syncSendBtn();
    });
  }
  if (sendBtn){ sendBtn.addEventListener('click', function(){ app.sendMsg(); }); }
  if (fileInput){ fileInput.addEventListener('change', function(e){ var files=[].slice.call(e.target.files||[]); if(files.length) app.sendFilesFrom(files); e.target.value=''; }); }

  if (sendArea){
    function onDragEnter(e){ e.preventDefault(); sendArea.classList.add('drag-over'); }
    function onDragOver(e){ e.preventDefault(); }
    function onDragLeave(e){ e.preventDefault(); if(e.target===sendArea || !sendArea.contains(e.relatedTarget)) sendArea.classList.remove('drag-over'); }
    function onDrop(e){ e.preventDefault(); sendArea.classList.remove('drag-over'); var files=[].slice.call((e.dataTransfer&&e.dataTransfer.files)||[]); if(files.length) app.sendFilesFrom(files); }
    sendArea.addEventListener('dragenter', onDragEnter);
    sendArea.addEventListener('dragover', onDragOver);
    sendArea.addEventListener('dragleave', onDragLeave);
    sendArea.addEventListener('drop', onDrop);
  }

  if (contactSearch){ contactSearch.addEventListener('input', function(){
    var arr=[]; for (var k in app.conns){ if(app.conns[k].open) arr.push({id:k,name: app.displayNames[k]||('节点 '+k.substring(0,8))}); }
    app._classic.renderContacts(arr, app.activePeer);
  }); }

  (function initialRender(){
    if (!contactList || !app || !app._classic || !app._classic.renderContacts) return;
    var arr = [];
    for (var pid in app.conns) {
      if (!app.conns.hasOwnProperty(pid)) continue;
      if (!app.conns[pid].open) continue;
      arr.push({ id: pid, name: app.displayNames[pid] || ('节点 ' + pid.substring(0,8)) });
    }
    app._classic.renderContacts(arr, app.activePeer);
  })();

  app._classic.updateStatus();
  try{ app.log('UI_READY'); }catch(e){}
}

// ===================== 播放模式芯片（覆盖版） =====================
(function bindStreamChip(){
  try{
    var header = document.querySelector('.header');
    var host = header || document.body;

    var chip = document.getElementById('streamChip');
    if (!chip){
      chip = document.createElement('span');
      chip.id = 'streamChip';
      chip.className = 'chip';
      chip.style.cursor = 'pointer';
      host.appendChild(chip);
    }

    function refresh(){
      try{
        chip.textContent = app.streamMode ? '在线播放' : '缓存观看';
        chip.title = '点击切换播放模式（在线播放 = WebRTC 直播；缓存观看 = 文件直传+缓存）';
      }catch(e){}
    }
    chip.onclick = function(){ try{ app.setStreamMode(!app.streamMode); refresh(); }catch(e){} };
    refresh();

    var onlineChip = document.getElementById('onlineChip');
    if (onlineChip){
      onlineChip.style.cursor = 'pointer';
      onlineChip.title = '点击切换 播放模式（在线播放/缓存观看）';
      onlineChip.onclick = function(){ try{ app.setStreamMode(!app.streamMode); refresh(); }catch(e){} };
    }
  }catch(e){ try{ app.log('STREAM_CHIP_EX: '+(e.message||e)); }catch(_){} }
})();
  (function addSpinCSS(){try{var css='.qq-audio .disc.spin{animation:disc-spin 1.2s linear infinite}@keyframes disc-spin{from{transform:rotate(0)}to{transform:rotate(360deg)}}.qq-audio .disc.playing{background:#2a7cff;color:#fff;border-color:#2a7cff}';var s=document.createElement('style');s.textContent=css;document.head.appendChild(s);}catch(e){}})();


// ===================== 启动/注入 =====================
if (window.CLASSIC_UI && window.opener) {
  (function waitOpener(){
    try{
      if (window.opener && window.opener.app) {
        window.app = window.opener.app;
        bindClassicUI(window.app);
        return;
      }
    }catch(e){}
    setTimeout(waitOpener, 200);
  })();
} else {
  window.app = app;
  bindClassicUI(app);
  if (!window.__ENTRY_PAGE__ && !app.isConnected && !app.__autostarted) { app.__autostarted = true; app.toggle(); }
}

// 顶层 IIFE 收口
})();