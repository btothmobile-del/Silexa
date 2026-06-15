self.addEventListener("push", event => {
  let data = { title: "Silexa", body: "Új briefing érhető el!" };
  try { data = JSON.parse(event.data.text()); } catch {}
  event.waitUntil(
    self.registration.showNotification(data.title, {
      body: data.body,
      icon: "/icon-192.png",
      badge: "/icon-192.png",
      data: { url: "/app.html" },
    })
  );
});

self.addEventListener("notificationclick", event => {
  event.notification.close();
  event.waitUntil(clients.openWindow(event.notification.data?.url || "/app.html"));
});
