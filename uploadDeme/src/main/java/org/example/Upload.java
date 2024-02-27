package org.example;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;

@RestController
public class Upload {
    @RequestMapping("/upload")
    public void upload(@RequestPart MultipartFile[] files) {
        System.out.println("begin upload " + files.length);
        String filePath;
        // 多文件上传
        for (MultipartFile file : files){
            // 上传简单文件名
            String originalFilename = file.getOriginalFilename();
            // 存储路径
            filePath = new StringBuilder("D:\\workspace\\javademo\\UploadDemo\\upload\\")
//                    .append(System.currentTimeMillis())
                    .append(originalFilename)
                    .toString();

            System.out.println("filePath = " + filePath);
            try {
                // 保存文件
                file.transferTo(new File(filePath));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
