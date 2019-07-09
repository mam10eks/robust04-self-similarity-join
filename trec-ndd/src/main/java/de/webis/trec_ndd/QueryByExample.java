package de.webis.trec_ndd;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class QueryByExample {
	public static void main(String[] args) {
		//warc_target_uri http://goldwest.visitmt.com/accommodations/camping/25/
		System.out.println(esQueryByExample("Camping and Gold West Country Montana go hand in hand. Whether you prefer to pitch a tent in the wilderness, or find a full hook up site for your RV, we have the perfect camp site for you! Our campgrounds offer whatever experience you are looking for - solitude with a view, or more amenities than you can imagine. Browse our listings and find the perfect spot for your Montana camping vacation. For FREE Gold West travel information call 1-800-879-1159 or request online. For information on linking to the Gold West Country website, click here. * If you know the name of the business or event you are looking, please make a selection"));
		
		System.out.println(esQueryByExample("Join deviantART for FREE Take the Tour Lost Password? This is a request for as she is awesome person to talk to XD Thanks for being a buddie! By the way Al is supposed to have a mouth pipe in his hand but I'm not the best drawer... A snake dreamt of soaring in the sky knowing full well it was impossible Still, in vain hope it kept its eye on a baby bird that it nurtured in its own nest... little realising... that the snake is prey because that bird is a hawk ready to take to the sky Yup this was for as she plays saxophone I felt bad for making her wait so long so it :/ Who cares about the name, HE'S HOT! A snake dreamt of soaring in the sky knowing full well it was impossible Still, in vain hope it kept its eye on a baby bird that it nurtured in its own nest... little realising... that the snake is prey because that bird is a hawk ready to take to the sky In our continuous effort to improve the deviantART experience, we're publishing Site Updates to keep members informed and to gather feedback. Below is a list of recent changes to the site, bug fixes, and feedback that was brought up by members in the last Site Update. I'm jumping on the giveaway bandwagon, wanna fight about it. But for real, it's cuz I :heart: you guys so much and I want to give :DSimple rules, no fuss. If you don't follow you won't have a valid entry. * Amazing Piece of Art + 50 DEVIANTS GET POINTS NOW! Hey Guys,I saw this and wanted to share it with you. It's off site, but is really worth taking a look. The image when blown up is just gorgeous! I hope you enjoy :)In addition! 50 deviants who comment all get points today from one awesome deviant! :iconsupereagerplz:A ll you have to do is comment... In which way do you prefer an adventure story hero to achieve his/her final victory? * Physical strength and fighting skills, and a little good luck * Through lone wolf personal action; breaking of rules and protocol Proudly showing 197 million pieces of art from over 20 million registered artists and art appreciators Deviously serving the art and skin community for 4,255 days Watch the official deviantART #hq Blog for news, product and feature releases, and awesome happenings: This month's Deviousness goes to a deviant who hasn't been on deviantART for a particularly long time, but he has taken the community by storm. `MajorGeneralWhiskers currently serves as the leader of Feline UpRising (FUR), which he rules with an iron paw. His commitment to his cause is unwavering, and he strives to make everyone else feel the same. We hail `MajorGeneralWhiskers as our new leader and the April 2012 Deviousness recipient! Read More About Us | Contact Us | Developers | Careers | Site Tour | Help & FAQ Advertise | Premium Membership Etiquette | Privacy Policy | Terms of Service | Copyright Policy"));
	}
	
	public static String esQueryByExample(String document){
		return tokensInText(document).stream()
				.map(t -> "(body_lang.en: \"" + t + "\")")
				.collect(Collectors.joining(" OR "));

	}

	private static Set<String> tokensInText(String text) {
		try (Analyzer analyzer = new StandardAnalyzer()) {
			return tokensInText(analyzer, text);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static Set<String> tokensInText(Analyzer analyzer, String text) throws IOException {
		Set<String> ret = new HashSet<>();
		TokenStream tokenStream = analyzer.tokenStream("", text);

		CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);
		tokenStream.reset();

		while (tokenStream.incrementToken()) {
			ret.add(attr.toString());
		}

		return ret;
	}
}
