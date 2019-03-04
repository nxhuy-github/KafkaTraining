var port = 8080;
 
var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);
 
 
//Create MongoWatch
var MongoWatch = require('mongo-oplog-watch');
 
var watcher = new MongoWatch(
    'mongodb://localhost:27017',
    { ns: 'streamdb.bike_aggregation' }    
);
 
//Watching for the insert action from MongoDB
watcher.on('insert', function(doc) {
    console.log("=============================");
    console.log(doc.object);
    console.log("-----------------------------");
    //If insert action is occured, emit data to client via socket.io
    io.emit("aggregator-message", doc.object)
});
 
app.get('/', function(req, res){
    //res.sendFile(path.join(__dirname, 'web', 'index.html'));
    //res.sendFile('index.html', { root: path.join(__dirname, 'web') });
    res.sendFile('web/index.html', {root: __dirname});
 
});
 
//Detech any user connect to server via web browser
io = io.on('connection', function(socket){
    console.log('a user connected');
    socket.on('disconnect', function(){
        console.log('a user disconnected');
    });
});
 
 
http.listen(port, function(){
    console.log("Running on port " + port)
});