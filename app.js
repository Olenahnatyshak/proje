const path = require('path');
const express = require('express');
const session = require('express-session');
const mysql = require('mysql2/promise');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// MySQL connection pool
const pool = mysql.createPool({
  host: process.env.DB_HOST || 'localhost',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'macfit_kds',
  charset: 'utf8mb4',
  waitForConnections: true,
  connectionLimit: 10
});

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use((req, res, next) => {
  res.locals.currentPath = req.path;
  next();
});
app.use(
  session({
    secret: process.env.SESSION_SECRET || 'macfit-kds-secret',
    resave: false,
    saveUninitialized: false,
    cookie: { maxAge: 1000 * 60 * 60 } // 1 hour
  })
);
app.use(express.static(path.join(__dirname, 'public')));

const ensureAuth = (req, res, next) => {
  if (!req.session?.kullanici) {
    return res.redirect('/');
  }
  next();
};

app.get('/', (req, res) => {
  if (req.session?.kullanici) {
    return res.redirect('/dashboard');
  }
  res.render('login', { hata: null });
});

app.post('/login', async (req, res) => {
  const { kullanici_adi, sifre } = req.body;
  if (!kullanici_adi || !sifre) {
    return res.render('login', { hata: 'Kullanıcı adı ve şifre gerekli.' });
  }
  try {
    const [rows] = await pool.execute(
      'SELECT id, kullanici_adi FROM kullanicilar WHERE kullanici_adi = ? AND sifre = ?',
      [kullanici_adi, sifre]
    );
    if (rows.length === 1) {
      req.session.kullanici = { id: rows[0].id, ad: rows[0].kullanici_adi };
      return res.redirect('/dashboard');
    }
    return res.render('login', { hata: 'Kullanıcı adı veya şifre hatalı.' });
  } catch (err) {
    console.error('Login error', err);
    return res.render('login', { hata: 'Giriş yapılırken hata oluştu.' });
  }
});

app.get('/logout', (req, res) => {
  req.session.destroy(() => {
    res.redirect('/');
  });
});

app.get('/dashboard', ensureAuth, async (req, res) => {
  const secili_ilce_id = parseInt(req.query.ilce_id, 10) || 0;
  const secili_sube_id = parseInt(req.query.sube_id, 10) || 0;
  const zaman_araligi = [6, 12].includes(parseInt(req.query.zaman_araligi, 10))
    ? parseInt(req.query.zaman_araligi, 10)
    : 12;

  const ilceListePromise = pool.query('SELECT ilce_id, ilce_adi FROM ilceler ORDER BY ilce_adi');
  const tumSubelerPromise = pool.query('SELECT sube_id, sube_adi, ilce_id FROM subeler ORDER BY ilce_id, sube_adi');

  const [ilce_liste] = await ilceListePromise;
  const [tum_subeler] = await tumSubelerPromise;

  let sube_listesi = [];
  if (secili_ilce_id > 0) {
    const [subeRows] = await pool.execute(
      'SELECT sube_id, sube_adi FROM subeler WHERE ilce_id = ? ORDER BY sube_adi',
      [secili_ilce_id]
    );
    sube_listesi = subeRows;
  }

  let ilce_adi = 'Tüm İzmir';
  let secili_sube_adi = secili_sube_id > 0 ? 'Seçili Şube' : '';
  let analiz_sonuclari = null;
  let grafik_verileri = [];
  let dagilim_sonuclari = {};

  if (secili_ilce_id > 0) {
    const [[ilceAdSatir]] = await pool.execute(
      'SELECT ilce_adi FROM ilceler WHERE ilce_id = ?',
      [secili_ilce_id]
    );
    ilce_adi = ilceAdSatir?.ilce_adi || ilce_adi;

    if (secili_sube_id > 0) {
      const [[subeSatir]] = await pool.execute(
        'SELECT sube_adi FROM subeler WHERE sube_id = ?',
        [secili_sube_id]
      );
      secili_sube_adi = subeSatir?.sube_adi || secili_sube_adi;
    }

    const condition = secili_sube_id > 0 ? ' AND s.sube_id = ?' : '';
    const params = secili_sube_id > 0 ? [secili_ilce_id, secili_sube_id] : [secili_ilce_id];

    // Tarih aralığını yıl-ay indeksleriyle hesapla (zaman dilimi kayması riskini yok sayıyoruz)
    const [[maxAyYilRow = {}]] = await pool.execute(
      `
        SELECT MAX(av.yil * 12 + (av.ay - 1)) AS max_index
        FROM aylik_veriler av
        JOIN subeler s ON av.sube_id = s.sube_id
        WHERE s.ilce_id = ?${condition}
      `,
      params
    );
    const bugunIndex = () => {
      const d = new Date();
      return d.getUTCFullYear() * 12 + d.getUTCMonth();
    };
    const maxIndex =
      Number.isFinite(Number(maxAyYilRow?.max_index)) && maxAyYilRow?.max_index !== null
        ? Number(maxAyYilRow.max_index)
        : bugunIndex();
    const basIndex = maxIndex - (zaman_araligi - 1);
    const paramsWithIndexes = [...params, basIndex, maxIndex];

    const veriSorgu = `
      SELECT 
        SUM(av.gelir) AS ToplamGelir,
        SUM(av.maliyet) AS ToplamMaliyet,
        AVG(av.aktif_uye_sayisi) AS OrtalamaAktifUye,
        SUM(av.ders_katilimci_sayisi) AS ToplamDersKatilimci,
        SUM(s.kapasite) AS ToplamKapasite
      FROM aylik_veriler av
      JOIN subeler s ON av.sube_id = s.sube_id
      WHERE s.ilce_id = ?${condition}
        AND (av.yil * 12 + (av.ay - 1)) BETWEEN ? AND ?
      GROUP BY s.ilce_id
    `;

    const grafikSorgu = `
      SELECT av.gelir, av.maliyet, av.aktif_uye_sayisi, av.ders_katilimci_sayisi, av.yil, av.ay
      FROM aylik_veriler av
      JOIN subeler s ON av.sube_id = s.sube_id
      WHERE s.ilce_id = ?${condition}
        AND (av.yil * 12 + (av.ay - 1)) BETWEEN ? AND ?
      ORDER BY av.yil ASC, av.ay ASC
    `;

    const dagilimSorgu = `
      SELECT 
        SUM(COALESCE(gd.uye_geliri, 0)) AS ToplamUyeGeliri,
        SUM(COALESCE(gd.ders_geliri, 0)) AS ToplamDersGeliri,
        SUM(COALESCE(gd.diger_gelir, 0)) AS ToplamDigerGelir,
        SUM(COALESCE(md.personel, 0)) AS ToplamPersonelMaliyeti,
        SUM(COALESCE(md.kira, 0)) AS ToplamKiraMaliyeti,
        SUM(COALESCE(md.elektrik, 0)) AS ToplamElektrikMaliyeti,
        SUM(COALESCE(md.su, 0)) AS ToplamSuMaliyeti,
        SUM(COALESCE(md.bakim, 0)) AS ToplamBakimMaliyeti,
        SUM(COALESCE(md.diger_maliyet, 0)) AS ToplamDigerMaliyet
      FROM aylik_veriler av
      JOIN subeler s ON av.sube_id = s.sube_id
      LEFT JOIN gelir_dagilimi gd ON av.veri_id = gd.veri_id
      LEFT JOIN maliyet_dagilimi md ON av.veri_id = md.veri_id
      WHERE s.ilce_id = ?${condition}
        AND (av.yil * 12 + (av.ay - 1)) BETWEEN ? AND ?
    `;

    const [[analizSatir]] = await pool.execute(veriSorgu, paramsWithIndexes);
    const [grafikRows] = await pool.execute(grafikSorgu, paramsWithIndexes);
    const [[dagilimSatir]] = await pool.execute(dagilimSorgu, paramsWithIndexes);

    analiz_sonuclari = analizSatir || null;
    dagilim_sonuclari = {
      ToplamUyeGeliri: Number(dagilimSatir?.ToplamUyeGeliri) || 0,
      ToplamDersGeliri: Number(dagilimSatir?.ToplamDersGeliri) || 0,
      ToplamDigerGelir: Number(dagilimSatir?.ToplamDigerGelir) || 0,
      ToplamPersonelMaliyeti: Number(dagilimSatir?.ToplamPersonelMaliyeti) || 0,
      ToplamKiraMaliyeti: Number(dagilimSatir?.ToplamKiraMaliyeti) || 0,
      ToplamElektrikMaliyeti: Number(dagilimSatir?.ToplamElektrikMaliyeti) || 0,
      ToplamSuMaliyeti: Number(dagilimSatir?.ToplamSuMaliyeti) || 0,
      ToplamBakimMaliyeti: Number(dagilimSatir?.ToplamBakimMaliyeti) || 0,
      ToplamDigerMaliyet: Number(dagilimSatir?.ToplamDigerMaliyet) || 0
    };

    if (analiz_sonuclari) {
      const toplKar = (analiz_sonuclari.ToplamGelir || 0) - (analiz_sonuclari.ToplamMaliyet || 0);
      const kapasite = analiz_sonuclari.ToplamKapasite || 0;
      analiz_sonuclari.ToplamKar = toplKar;
      analiz_sonuclari.KapasiteKullanimi = kapasite > 0
        ? ((analiz_sonuclari.OrtalamaAktifUye || 0) / kapasite) * 100
        : 0;
    }

    const aylik_toplamlar = {};
    grafikRows.forEach((veri) => {
      const anahtar = `${veri.yil}-${String(veri.ay).padStart(2, '0')}`;
      if (!aylik_toplamlar[anahtar]) {
        aylik_toplamlar[anahtar] = {
          yil: Number(veri.yil),
          ay: Number(veri.ay),
          gelir: 0,
          maliyet: 0,
          aktif_uye_sayisi: 0,
          ders_katilimci_sayisi: 0
        };
      }
      aylik_toplamlar[anahtar].gelir += Number(veri.gelir);
      aylik_toplamlar[anahtar].maliyet += Number(veri.maliyet);
      aylik_toplamlar[anahtar].aktif_uye_sayisi += Number(veri.aktif_uye_sayisi);
      aylik_toplamlar[anahtar].ders_katilimci_sayisi += Number(veri.ders_katilimci_sayisi);
    });

    const sirali = Object.entries(aylik_toplamlar)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([, v]) => v);
    grafik_verileri = sirali.slice(-zaman_araligi);
  }

  res.render('dashboard', {
    kullanici: req.session.kullanici,
    ilce_liste,
    tum_subeler,
    sube_listesi,
    secili_ilce_id,
    secili_sube_id,
    zaman_araligi,
    ilce_adi,
    secili_sube_adi,
    analiz_sonuclari,
    grafik_verileri,
    dagilim_sonuclari
  });
});

app.get('/performans-ozeti', ensureAuth, async (req, res) => {
  const zaman_araligi = [6, 12].includes(parseInt(req.query.zaman_araligi, 10))
    ? parseInt(req.query.zaman_araligi, 10)
    : 12;

  const [maxRows] = await pool.query(
    "SELECT MAX(STR_TO_DATE(CONCAT(yil,'-',LPAD(ay,2,'0'),'-01'), '%Y-%m-%d')) AS max_tarih FROM aylik_veriler"
  );
  const [{ max_tarih } = {}] = maxRows;
  const bitis_tarihi = max_tarih ? new Date(max_tarih) : new Date();
  const baslangic_tarihi = new Date(bitis_tarihi);
  baslangic_tarihi.setMonth(bitis_tarihi.getMonth() - (zaman_araligi - 1));

  const formatDate = (d) => d.toISOString().slice(0, 10);

  const [sonuclar] = await pool.execute(
    `
    SELECT 
        i.ilce_adi,
        COUNT(DISTINCT s.sube_id) AS SubeSayisi,
        SUM(av.gelir) AS ToplamGelir,
        SUM(av.maliyet) AS ToplamMaliyet,
        AVG(av.aktif_uye_sayisi) AS OrtalamaAktifUye,
        SUM(s.kapasite) AS ToplamKapasite
    FROM ilceler i
    JOIN subeler s ON i.ilce_id = s.ilce_id
    JOIN (
        SELECT 
            av.*,
            STR_TO_DATE(CONCAT(av.yil,'-',LPAD(av.ay,2,'0'),'-01'), '%Y-%m-%d') AS veri_tarihi
        FROM aylik_veriler av
        WHERE STR_TO_DATE(CONCAT(av.yil,'-',LPAD(av.ay,2,'0'),'-01'), '%Y-%m-%d') BETWEEN ? AND ?
    ) av ON s.sube_id = av.sube_id
    GROUP BY i.ilce_adi
    ORDER BY SubeSayisi DESC
    `,
    [formatDate(baslangic_tarihi), formatDate(bitis_tarihi)]
  );

  res.render('performans_ozeti', {
    kullanici: req.session.kullanici,
    zaman_araligi,
    sonuclar
  });
});

app.get('/oneriler', ensureAuth, async (req, res) => {
  const secili_ilce_id = parseInt(req.query.ilce_id, 10) || 0;
  const secili_sube_id = parseInt(req.query.sube_id, 10) || 0;
  const zaman_araligi = [6, 12].includes(parseInt(req.query.zaman_araligi, 10))
    ? parseInt(req.query.zaman_araligi, 10)
    : 12;
  const ay_baslangic = Math.max(1, 13 - zaman_araligi);
  const secili_oneri_tipi = req.query.oneri_tipi || 'all';

  const [ilceler] = await pool.query('SELECT ilce_id, ilce_adi FROM ilceler ORDER BY ilce_adi');
  const [tum_subeler] = await pool.query('SELECT sube_id, sube_adi, ilce_id FROM subeler ORDER BY ilce_id, sube_adi');

  let sube_listesi = [];
  if (secili_ilce_id > 0) {
    const [rows] = await pool.execute(
      'SELECT sube_id, sube_adi FROM subeler WHERE ilce_id = ? ORDER BY sube_adi',
      [secili_ilce_id]
    );
    sube_listesi = rows;
  }

  let where = 'av.ay >= ?';
  const params = [ay_baslangic];

  if (secili_ilce_id > 0) {
    where += ' AND i.ilce_id = ?';
    params.push(secili_ilce_id);
  }
  if (secili_sube_id > 0) {
    where += ' AND s.sube_id = ?';
    params.push(secili_sube_id);
  }

  const [veriRows] = await pool.execute(
    `
    SELECT 
        s.sube_id,
        s.sube_adi,
        i.ilce_adi,
        s.kapasite,
        SUM(av.gelir) AS ToplamGelir,
        SUM(av.maliyet) AS ToplamMaliyet,
        AVG(av.aktif_uye_sayisi) AS OrtalamaAktifUye
    FROM subeler s
    JOIN ilceler i ON s.ilce_id = i.ilce_id
    JOIN aylik_veriler av ON s.sube_id = av.sube_id
    WHERE ${where}
    GROUP BY s.sube_id, s.sube_adi, i.ilce_adi, s.kapasite
    ORDER BY i.ilce_adi ASC, s.sube_adi ASC
    `,
    params
  );

  const oneriler = [];
  veriRows.forEach((satir) => {
    const toplam_kar = (satir.ToplamGelir || 0) - (satir.ToplamMaliyet || 0);
    const karlilik_orani = satir.ToplamGelir > 0 ? (toplam_kar / satir.ToplamGelir) * 100 : 0;
    const kapasite_doluluk = satir.kapasite > 0 ? (satir.OrtalamaAktifUye / satir.kapasite) * 100 : 0;

    if (kapasite_doluluk >= 92 && karlilik_orani >= 25) {
      oneriler.push({
        tip: 'ac',
        baslik: 'Yeni Şube Açma Önerisi',
        metin: `${satir.ilce_adi} ilçesindeki ${satir.sube_adi}, %${kapasite_doluluk.toFixed(
          1
        )} doluluk ve %${karlilik_orani.toFixed(
          1
        )} karlılıkla sınırda çalışıyor. Aynı ilçede talebi dengelemek ve risk dağıtmak için yeni bir şube açılması değerlendirilebilir.`
      });
      return;
    }

    if (karlilik_orani <= 5 || (kapasite_doluluk < 40 && karlilik_orani < 12)) {
      oneriler.push({
        tip: 'kapat',
        baslik: 'Kapatma / Taşıma Önerisi',
        metin: `${satir.sube_adi} (${satir.ilce_adi}) için karlılık %${karlilik_orani.toFixed(
          1
        )}, doluluk %${kapasite_doluluk.toFixed(
          1
        )}. Az müşteri ve düşük kar nedeniyle şubenin kapatılması veya başka bir bölgede daha küçük bir formatta konumlandırılması değerlendirilmeli.`
      });
      return;
    }

    oneriler.push({
      tip: 'pazarlama',
      baslik: 'İyileştirme Önerisi',
      metin: `${satir.sube_adi} (${satir.ilce_adi}) için karlılık %${karlilik_orani.toFixed(
        1
      )} ve doluluk %${kapasite_doluluk.toFixed(
        1
      )}. Pazarlama kampanyası veya sınıf/üyelik paketleri ile talep artırılıp karlılık korunabilir.`
    });
  });

  // İlçede şube yoksa nüfusa göre öneri
  const ilceFilter = secili_ilce_id > 0 ? 'WHERE i.ilce_id = ?' : '';
  const ilceParams = secili_ilce_id > 0 ? [secili_ilce_id] : [];
  const [ilceRows] = await pool.execute(
    `
    SELECT i.ilce_id, i.ilce_adi, i.nufus, COUNT(s.sube_id) AS sube_sayisi
    FROM ilceler i
    LEFT JOIN subeler s ON i.ilce_id = s.ilce_id
    ${ilceFilter}
    GROUP BY i.ilce_id, i.ilce_adi, i.nufus
    HAVING COUNT(s.sube_id) = 0
    `,
    ilceParams
  );

  ilceRows.forEach((row) => {
    const nufus = Number(row.nufus) || 0;
    if (nufus >= 45000) {
      oneriler.push({
        tip: 'ac',
        extra_class: 'ac-nufus',
        baslik: 'Yeni Şube Açılışı Önerisi (Nüfus)',
        metin: `${row.ilce_adi} ilçesinde şube bulunmuyor. Nüfus ${nufus.toLocaleString(
          'tr-TR'
        )} kişi; talebi karşılamak ve varlık göstermek için bu bölgede en az bir şube açılması değerlendirilebilir.`
      });
    } else {
      oneriler.push({
        tip: 'ac',
        extra_class: 'acmama-nufus',
        baslik: 'Şube Açmama Önerisi (Nüfus)',
        metin: `${row.ilce_adi} ilçesinde nüfus ${nufus.toLocaleString(
          'tr-TR'
        )} kişi ve şube bulunmuyor. Talep oluşumunu görmek için saha araştırması, pazarlama testleri ve maliyet analizi sonrasında şube açılışı ertelenmelidir.`
      });
    }
  });

  const filtreliOneriler =
    secili_oneri_tipi === 'all'
      ? oneriler
      : oneriler.filter((o) => o.tip === secili_oneri_tipi);

  res.render('oneriler', {
    kullanici: req.session.kullanici,
    ilceler,
    tum_subeler,
    sube_listesi,
    secili_ilce_id,
    secili_sube_id,
    secili_oneri_tipi,
    zaman_araligi,
    oneriler: filtreliOneriler
  });
});

app.get('/harita', ensureAuth, async (req, res) => {
  const secili_ilce_id = parseInt(req.query.ilce_id, 10) || 0;
  const secili_sube_id = parseInt(req.query.sube_id, 10) || 0;
  const secili_ay = parseInt(req.query.ay, 10) || 0;

  const ilce_id_map = {};
  const ilce_slug_map = {};
  const [ilceListe] = await pool.query('SELECT ilce_id, ilce_adi FROM ilceler ORDER BY ilce_adi');
  ilceListe.forEach((row) => {
    ilce_id_map[row.ilce_id] = row.ilce_adi;
    ilce_slug_map[ilceSlug(row.ilce_adi)] = row.ilce_id;
  });

  const [tum_subeler] = await pool.query('SELECT sube_id, sube_adi, ilce_id FROM subeler ORDER BY ilce_id, sube_adi');

  const [haritaRows] = await pool.query(
    `
    SELECT 
        i.ilce_id,
        i.ilce_adi,
        s.sube_id,
        s.sube_adi,
        av.yil,
        av.ay,
        SUM(av.gelir) AS toplam_gelir,
        SUM(av.maliyet) AS toplam_maliyet,
        SUM(av.aktif_uye_sayisi) AS toplam_aktif_uye,
        i.nufus AS nufus
    FROM aylik_veriler av
    JOIN subeler s ON av.sube_id = s.sube_id
    JOIN ilceler i ON s.ilce_id = i.ilce_id
    GROUP BY s.sube_id, s.sube_adi, i.ilce_id, i.ilce_adi, av.yil, av.ay
    ORDER BY av.yil ASC, av.ay ASC, i.ilce_adi ASC, s.sube_adi ASC
    `
  );

  const aylar = {
    1: 'Ocak',
    2: 'Subat',
    3: 'Mart',
    4: 'Nisan',
    5: 'Mayis',
    6: 'Haziran',
    7: 'Temmuz',
    8: 'Agustos',
    9: 'Eylul',
    10: 'Ekim',
    11: 'Kasim',
    12: 'Aralik'
  };

  res.render('harita', {
    kullanici: req.session.kullanici,
    secili_ilce_id,
    secili_sube_id,
    secili_ay,
    ilce_id_map,
    ilce_slug_map,
    tum_subeler,
    harita_verileri: haritaRows,
    aylar
  });
});

app.listen(PORT, () => {
  console.log(`MacFit KDS Node is running at http://localhost:${PORT}`);
});

function ilceSlug(text) {
  const map = {
    "\u00c7": "C",
    "\u00e7": "C",
    "\u011e": "G",
    "\u011f": "G",
    "\u0130": "I",
    "\u0131": "I",
    "\u00d6": "O",
    "\u00f6": "O",
    "\u015e": "S",
    "\u015f": "S",
    "\u00dc": "U",
    "\u00fc": "U"
  };
  return (text || '')
    .replace(/[\u00c7\u00e7\u011e\u011f\u0130\u0131\u00d6\u00f6\u015e\u015f\u00dc\u00fc]/g, (c) => map[c] || c)
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '');
}
