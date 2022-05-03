"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getVideoDuration = exports.fetchYoutubeVideo = exports.getYoutubeVideoID = exports.searchYoutube = exports.mapYoutubeListResult = exports.mapYoutubeSearchResult = void 0;
const config_1 = __importDefault(require("../config"));
const youtube_api_1 = __importDefault(require("youtube-api"));
const regex_1 = require("./regex");
if (config_1.default.YOUTUBE_API_KEY) {
    youtube_api_1.default.authenticate({
        type: 'key',
        key: config_1.default.YOUTUBE_API_KEY,
    });
}
const mapYoutubeSearchResult = (video) => {
    return {
        channel: video.snippet.channelTitle,
        url: 'https://www.youtube.com/watch?v=' + video.id.videoId,
        name: video.snippet.title,
        img: video.snippet.thumbnails.default.url,
        duration: 0,
    };
};
exports.mapYoutubeSearchResult = mapYoutubeSearchResult;
const mapYoutubeListResult = (video) => {
    var _a;
    const videoId = video.id;
    return {
        url: 'https://www.youtube.com/watch?v=' + videoId,
        name: video.snippet.title,
        img: video.snippet.thumbnails.default.url,
        channel: video.snippet.channelTitle,
        duration: (_a = (0, exports.getVideoDuration)(video.contentDetails.duration)) !== null && _a !== void 0 ? _a : 0,
    };
};
exports.mapYoutubeListResult = mapYoutubeListResult;
const searchYoutube = (query) => {
    return new Promise((resolve, reject) => {
        youtube_api_1.default.search.list({ part: 'snippet', type: 'video', maxResults: 25, q: query }, (err, data) => {
            if (data && data.items) {
                const response = data.items.map(exports.mapYoutubeSearchResult);
                resolve(response);
            }
            else {
                console.warn(data);
                reject();
            }
        });
    });
};
exports.searchYoutube = searchYoutube;
const getYoutubeVideoID = (url) => {
    const idParts = regex_1.YOUTUBE_VIDEO_ID_REGEX.exec(url);
    if (!idParts) {
        return;
    }
    const id = idParts[1];
    if (!id) {
        return;
    }
    return id;
};
exports.getYoutubeVideoID = getYoutubeVideoID;
const fetchYoutubeVideo = (id) => {
    return new Promise((resolve, reject) => {
        youtube_api_1.default.videos.list({ part: 'snippet,contentDetails', id }, (err, data) => {
            if (data) {
                const video = data.items[0];
                resolve((0, exports.mapYoutubeListResult)(video));
            }
            else {
                console.warn(err);
                reject('unknown youtube api error');
            }
        });
    });
};
exports.fetchYoutubeVideo = fetchYoutubeVideo;
const getVideoDuration = (string) => {
    const hoursParts = regex_1.PT_HOURS_REGEX.exec(string);
    const minutesParts = regex_1.PT_MINUTES_REGEX.exec(string);
    const secondsParts = regex_1.PT_SECONDS_REGEX.exec(string);
    const hours = hoursParts ? parseInt(hoursParts[1]) : 0;
    const minutes = minutesParts ? parseInt(minutesParts[1]) : 0;
    const seconds = secondsParts ? parseInt(secondsParts[1]) : 0;
    const totalSeconds = seconds + minutes * 60 + hours * 60 * 60;
    return totalSeconds;
};
exports.getVideoDuration = getVideoDuration;
