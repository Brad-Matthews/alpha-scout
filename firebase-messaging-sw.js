// Firebase Cloud Messaging Service Worker
// IMPORTANT: Replace the firebaseConfig values below with your own
// from Firebase Console > Project Settings > Your Apps > Web App.
// These MUST match the values in fcm_token_generator.html.

importScripts('https://www.gstatic.com/firebasejs/10.12.0/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/10.12.0/firebase-messaging-compat.js');

firebase.initializeApp({
    apiKey: "AIzaSyAdYSov6-Hx48Zp4QyiTHgtJf776pXUJKI",
    authDomain: "summed-b6f19.firebaseapp.com",
    databaseURL: "https://summed-b6f19.firebaseio.com",
    projectId: "summed-b6f19",
    storageBucket: "summed-b6f19.firebasestorage.app",
    messagingSenderId: "32835619262",
    appId: "1:32835619262:web:71a625dcf44ab447b891ef"
});

const messaging = firebase.messaging();
